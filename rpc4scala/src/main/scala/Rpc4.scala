package org.stingray.contester.rpc4

import com.google.protobuf.MessageLite
import com.twitter.util.{Future, Promise}
import org.jboss.netty.buffer.{ChannelBuffers, ChannelBuffer}
import org.jboss.netty.channel._
import org.jboss.netty.handler.codec.frame.FrameDecoder
import org.stingray.contester.utils.ProtobufTools
import org.stingray.contester.rpc4.proto.RpcFour
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.{AtomicInteger, AtomicBoolean}
import grizzled.slf4j.Logging

/** Connected server registry. Will be called for connected and disconnected channels.
  *
  */
trait Registry {
  /** This gets called when new server reverse-connects to the dispatcher.
    *
    * @param client Client for connected channel.
    */
  def register(client: RpcClient): Unit

  /** This gets called when channel is ejected from the dispatcher.
    *
    * @param client Client instance.
    */
  def unregister(client: RpcClient): Unit
}

/** Exception to be thrown when channel is disconnected.
  *
  * @param reason Reason received from netty.
  */
class ChannelDisconnectedException(reason: scala.Throwable) extends scala.Throwable(reason) {
  def this() =
    this(new Throwable)
}

/** Error in the remote server.
  *
  * @param value String passed as error description.
  */
class RemoteError(value: String) extends RuntimeException(value)

/** Dispatcher's pipeline factory. Will produce a pipeline that speaks rpc4 and connects those to the registry.
  *
  * @param registry Where do we register our channels.
  */
class ServerPipelineFactory(registry: Registry) extends ChannelPipelineFactory {
  def getPipeline = {
    val result = Channels.pipeline()
    result.addFirst("Registerer", new RpcRegisterer(registry))
    result.addFirst("RpcDecoder", new RpcFramerDecoder)
    result.addFirst("RpcEncoder", new RpcFramerEncoder)
    result.addFirst("FrameDecoder", new SimpleFramer)

    result
  }
}

/** Framer for our protocol.
  * Reads frames prefixed by 4-byte length.
  */
private class SimpleFramer extends FrameDecoder {
  def decode(ctx: ChannelHandlerContext, chan: Channel, buf: ChannelBuffer) = {
    if (buf.readableBytes() > 4) {
      buf.markReaderIndex()
      val length = buf.readInt()
      if (buf.readableBytes() < length) {
        buf.resetReaderIndex()
        null
      } else {
        buf.readBytes(length)
      }
    } else null
  }
}

/** Decoded messages, contain header and optionally payload.
  *
  * @param header
  * @param payload
  */
private class Rpc4Tuple(val header: RpcFour.Header, val payload: Option[Array[Byte]])

/** Decoder. A message has a header and optionally a payload.
  *
  */
private class RpcFramerDecoder extends SimpleChannelUpstreamHandler {
  private[this] var storedHeader: Option[RpcFour.Header] = None

  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent): Unit = {
    val frame = e.getMessage.asInstanceOf[ChannelBuffer]
    storedHeader.synchronized {
      if (storedHeader.isEmpty) {
        val header = RpcFour.Header.parseFrom(frame.array())
        if (header.getPayloadPresent) {
          storedHeader = Some(header)
        } else {
          Channels.fireMessageReceived(ctx, new Rpc4Tuple(header, None))
        }
      } else {
        val header = storedHeader.get
        storedHeader = None
        Channels.fireMessageReceived(ctx, new Rpc4Tuple(header, Some(frame.array())))
      }
    }
  }
}

/** Encoder.
  *
  */
private class RpcFramerEncoder extends SimpleChannelDownstreamHandler {
  private[this] class JustReturnListener(e: MessageEvent) extends ChannelFutureListener {
    def operationComplete(p1: ChannelFuture) {
      if (p1.isSuccess) {
        e.getFuture.setSuccess()
      } else e.getFuture.setFailure(p1.getCause)
    }
  }

  private[this] def withLength(channel: Channel, x: Array[Byte]) = {
    val b = ChannelBuffers.wrappedBuffer(x)
    val header = channel.getConfig.getBufferFactory.getBuffer(b.order(), 4)
    header.writeInt(b.readableBytes())
    ChannelBuffers.wrappedBuffer(header, b)
  }

  override def writeRequested(ctx: ChannelHandlerContext, e: MessageEvent) {
    val rpc = e.getMessage.asInstanceOf[Rpc4Tuple]
    val cf = Channels.future(e.getChannel)
    val buffer = ChannelBuffers.wrappedBuffer(
      (List(withLength(e.getChannel, rpc.header.toByteArray)) ++ rpc.payload.map(withLength(e.getChannel, _))):_*)
    cf.addListener(new JustReturnListener(e))
    Channels.write(ctx, cf, buffer)
  }
}

private class RpcRegisterer(registry: Registry) extends SimpleChannelUpstreamHandler {
  private[this] val channels = {
    import scala.collection.JavaConverters._
    new ConcurrentHashMap[Channel, RpcClient]().asScala
  }

  override def channelConnected(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    val channel = e.getChannel
    val client = new RpcClient(channel)

    channels.put(channel, client).foreach(registry.unregister(_))
    ctx.getPipeline.addLast("endpoint", client)
    super.channelConnected(ctx, e)
    registry.register(client)
  }

  override def channelDisconnected(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    channels.remove(e.getChannel).foreach(registry.unregister(_))
    super.channelDisconnected(ctx, e)
  }
}

/** RPC Client over the channel given.
  * Offers a Future-based call interface.
  * @param channel Channel to work on.
  */
class RpcClient(val channel: Channel) extends SimpleChannelUpstreamHandler with Logging {
  // todo: implement org.stingray.contester.rpc4.RpcClient.exceptionCaught()

  trace("Creating new rpc client for channel %s".format(channel))

  private[this] val requests = {
    import scala.collection.JavaConverters._
    new ConcurrentHashMap[Int, Promise[Option[Array[Byte]]]]().asScala
  }
  private[this] val sequenceNumber = new AtomicInteger
  private[this] val disconnected = new AtomicBoolean

  private class WriteHandler(requestId: Int) extends ChannelFutureListener {
    def operationComplete(p1: ChannelFuture) {
      if (!p1.isSuccess) {
        // Any write exception is a channel disconnect
        requests.remove(requestId).map(
          _.setException(new ChannelDisconnectedException(p1.getCause))
        )
      }
    }
  }

  private[this] def callUntyped(methodName: String, payload: Option[Array[Byte]]): Future[Option[Array[Byte]]] = {
    if (disconnected.get())
      Future.exception(new ChannelDisconnectedException)
    else {
      val result = new Promise[Option[Array[Byte]]]
      val requestId = sequenceNumber.getAndIncrement
      val header = RpcFour.Header.newBuilder().setMessageType(RpcFour.Header.MessageType.REQUEST)
        .setMethod(methodName).setPayloadPresent(payload.isDefined).setSequence(requestId).build()

      requests.put(requestId, result)

      Future {
        Channels.write(channel, new Rpc4Tuple(header, payload)).addListener(new WriteHandler(requestId))
      }.flatMap(_ => result).rescue {
        // Any write exception is a channel disconnect
        case e: Throwable =>
          requests.remove(requestId, result)
          Future.exception(new ChannelDisconnectedException(e))
      }
    }
  }

  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
    val rpc = e.getMessage.asInstanceOf[Rpc4Tuple]
    rpc.header.getMessageType match  {
      case RpcFour.Header.MessageType.RESPONSE =>
        requests.remove(rpc.header.getSequence.toInt).map(_.setValue(rpc.payload))
      case RpcFour.Header.MessageType.ERROR =>
        requests.remove(rpc.header.getSequence.toInt).map(
          _.setException(new RemoteError(rpc.payload.map(new String(_, "UTF-8")).getOrElse("Unspecified error"))))
      case _ =>
        super.messageReceived(ctx, e)
    }
  }

  override def channelDisconnected(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    disconnected.set(true)
    while (requests.nonEmpty) {
      requests.keys.flatMap(requests.remove(_)).foreach(_.setException(new ChannelDisconnectedException))
    }
    super.channelDisconnected(ctx, e)
  }

  private[this] def callTrace[A](methodName: String, payload: Option[MessageLite])(f: Option[Array[Byte]] => A) = {
    trace("Call: %s(%s)".format(methodName, payload))
    callUntyped(methodName, payload.map(_.toByteArray)).map(f(_))
      .onSuccess(result => trace("Result(%s): %s".format(methodName, result)))
      .onFailure(error("Error(%s)", _))
  }

  def call[I <: com.google.protobuf.Message](methodName: String, payload: MessageLite)(implicit manifest: Manifest[I]) =
    callTrace(methodName, Some(payload))(v => ProtobufTools.createProtobuf[I](v))

  def call[I <: com.google.protobuf.Message](methodName: String)(implicit manifest: Manifest[I]) =
    callTrace(methodName, None)(v => ProtobufTools.createProtobuf[I](v))

  def callNoResult(methodName: String, payload: MessageLite) =
    callTrace(methodName, Some(payload))(_ => ())

  def callNoResult(methodName: String) =
    callTrace(methodName, None)(_ => ())
}
