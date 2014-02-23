package org.stingray.contester.rpc4

import org.jboss.netty.handler.codec.frame.FrameDecoder
import org.jboss.netty.channel._
import org.jboss.netty.buffer.{ChannelBuffers, ChannelBuffer}
import org.stingray.contester.rpc4.proto.Rpcfour
import scala.Some

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
private class Rpc4Tuple(val header: Rpcfour.Header, val payload: Option[Array[Byte]]) {
  def asChannelBuffer(channel: Channel) =
    ChannelBuffers.wrappedBuffer(
      (List(Rpc4Tuple.withLength(channel, header.toByteArray)) ++ payload.map(Rpc4Tuple.withLength(channel, _))):_*)
}

private object Rpc4Tuple {
  def withLength(channel: Channel, x: Array[Byte]) = {
    val b = ChannelBuffers.wrappedBuffer(x)
    val header = channel.getConfig.getBufferFactory.getBuffer(b.order(), 4)
    header.writeInt(b.readableBytes())
    ChannelBuffers.wrappedBuffer(header, b)
  }
}

/** Decoder. A message has a header and optionally a payload.
  *
  */
private class RpcFramerDecoder extends SimpleChannelUpstreamHandler {
  private[this] var storedHeader: Option[Rpcfour.Header] = None

  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent): Unit = {
    val frame = e.getMessage.asInstanceOf[ChannelBuffer]
    storedHeader.synchronized {
      if (storedHeader.isEmpty) {
        val header = Rpcfour.Header.parseFrom(frame.array())
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

  override def writeRequested(ctx: ChannelHandlerContext, e: MessageEvent) {
    val message = e.getMessage match {
      case rpc: Rpc4Tuple => rpc.asChannelBuffer(e.getChannel)
    }

    val cf = Channels.future(e.getChannel)
    cf.addListener(new JustReturnListener(e))
    Channels.write(ctx, cf, message)
  }
}
