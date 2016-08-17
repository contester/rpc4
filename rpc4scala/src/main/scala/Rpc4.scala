package org.stingray.contester.rpc4

import com.google.protobuf.MessageLite
import com.twitter.util.{Future, Promise}
import org.jboss.netty.buffer.{ChannelBuffers, ChannelBuffer}
import org.jboss.netty.channel._
import org.jboss.netty.handler.codec.frame.FrameDecoder
import org.stingray.contester.rpc4.proto.Rpcfour
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.{AtomicInteger, AtomicBoolean}

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
class RpcClient(val channel: Channel) extends SimpleChannelUpstreamHandler {
  // todo: implement org.stingray.contester.rpc4.RpcClient.exceptionCaught()

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
      val header = Rpcfour.Header.newBuilder().setMessageType(Rpcfour.Header.MessageType.REQUEST)
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
      case Rpcfour.Header.MessageType.RESPONSE =>
        requests.remove(rpc.header.getSequence.toInt).map(_.setValue(rpc.payload))
      case Rpcfour.Header.MessageType.ERROR =>
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
}
