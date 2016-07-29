package com.fotolog.redis.connections

import java.net.InetSocketAddress
import java.nio.charset.Charset
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import java.util.concurrent._

import com.fotolog.redis._
import org.jboss.netty.bootstrap.ClientBootstrap
import org.jboss.netty.buffer._
import org.jboss.netty.channel.ChannelHandler.Sharable
import org.jboss.netty.channel._
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory
import org.jboss.netty.handler.codec.frame.FrameDecoder
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

object Netty3RedisConnection {

  private[redis] val executor = Executors.newCachedThreadPool()
  private[redis] val channelFactory = new NioClientSocketChannelFactory(executor, executor)
  private[redis] val commandEncoder = new RedisCommandEncoder() // stateless
  private[redis] val cmdQueue = new ArrayBlockingQueue[(Netty3RedisConnection, ResultFuture)](2048)

  private[redis] val queueProcessor = new Runnable {
    override def run() = {
      while(true) {
        val (conn, f) = cmdQueue.take()
        try {
          if (conn.isOpen) {
            conn.enqueue(f)
          } else {
            f.promise.failure(new IllegalStateException("Channel closed, command: " + f.cmd))
          }
        } catch {
          case e: Exception => f.promise.failure(e); conn.shutdown()
        }
      }
    }
  }

  new Thread(queueProcessor, "Queue Processor").start()
}

class Netty3RedisConnection(val host: String, val port: Int) extends RedisConnection {

  import com.fotolog.redis.connections.Netty3RedisConnection._

  private[Netty3RedisConnection] val isRunning = new AtomicBoolean(true)
  private[Netty3RedisConnection] val clientBootstrap = new ClientBootstrap(channelFactory)
  private[Netty3RedisConnection] val opQueue =  new ArrayBlockingQueue[ResultFuture](1028)
  private[Netty3RedisConnection] var clientState = new AtomicReference[ConnectionState](NormalConnectionState(opQueue))

  clientBootstrap.setPipelineFactory(new ChannelPipelineFactory() {
    override def getPipeline = {
      val p = Channels.pipeline
      p.addLast("response_decoder",     new RedisResponseDecoder())
      p.addLast("response_accumulator", new RedisResponseAccumulator(clientState))
      p.addLast("command_encoder",      commandEncoder)
      p
    }
  })

  clientBootstrap.setOption("tcpNoDelay", true)
  clientBootstrap.setOption("keepAlive", true)
  clientBootstrap.setOption("connectTimeoutMillis", 1000)

  private [Netty3RedisConnection] val channelReady = new CountDownLatch(1)

  private[Netty3RedisConnection] val channel = {
    val future = clientBootstrap.connect(new InetSocketAddress(host, port))
    var channelInternal: Channel = null

    future.addListener(new ChannelFutureListener() {
      override def operationComplete(channelFuture: ChannelFuture) = {
        if (future.isSuccess) {
          channelInternal = future.getChannel
          channelReady.countDown()
        } else {
          isRunning.set(false)
          channelReady.countDown()
          throw future.getCause
        }
      }
    })

    try {
      channelReady.await(1, TimeUnit.MINUTES)
    } catch {
      case iex: InterruptedException =>
        throw new RedisException("Interrupted while waiting for connection")
    }

    channelInternal
  }

  def send(cmd: Cmd): Future[Result] = {
    if (!isRunning.get()) {
      throw new RedisException("Connection closed")
    }
    val f = ResultFuture(cmd)
    cmdQueue.offer((this, f), 10, TimeUnit.SECONDS)
    f.future
  }

  def enqueue(f: ResultFuture) {
    opQueue.offer(f, 10, TimeUnit.SECONDS)
    channel.write(f).addListener(ChannelFutureListener.CLOSE_ON_FAILURE)
  }

  def isOpen: Boolean = isRunning.get() && channel.isOpen

  def shutdown() {
    if (isOpen) {
      isRunning.set(false)
      val channelClosed = new CountDownLatch(1)

      channel.close().addListener(new ChannelFutureListener() {
        override def operationComplete(channelFuture: ChannelFuture) = {
          channelClosed.countDown()
        }
      })

      try {
        channelClosed.await(1, TimeUnit.MINUTES)
      } catch {
        case iex: InterruptedException =>
          throw new RedisException("Interrupted while waiting for connection close")
      }
    }
  }
}

@Sharable
private[redis] class RedisCommandEncoder extends OneToOneEncoder {
  import com.fotolog.redis.connections.Cmd._
  import org.jboss.netty.buffer.ChannelBuffers._

  override def encode(ctx: ChannelHandlerContext, channel: Channel, msg: AnyRef): AnyRef = {
    val opFuture = msg.asInstanceOf[ResultFuture]
    binaryCmd(opFuture.cmd.asBin)
  }

  private def binaryCmd(cmdParts: Seq[Array[Byte]]): ChannelBuffer = {
    val params = new Array[Array[Byte]](3*cmdParts.length + 1)
    params(0) = ("*" + cmdParts.length + "\r\n").getBytes // num binary chunks
    var i = 1
    for(p <- cmdParts) {
      params(i) = ("$" + p.length + "\r\n").getBytes // len of the chunk
      i = i+1
      params(i) = p
      i = i+1
      params(i) = EOL
      i = i+1
    }
    copiedBuffer(params: _*)
  }
}

private[redis] trait ChannelExceptionHandler {
  def handleException(ctx: ChannelHandlerContext, e: ExceptionEvent) {
    e.getChannel.close() // don't allow any more ops on this channel, pipeline is busted
  }
}

private[redis] class RedisResponseDecoder extends FrameDecoder with ChannelExceptionHandler {

  val EOL_FINDER = new ChannelBufferIndexFinder() {
    override def find(buf: ChannelBuffer, pos: Int): Boolean = {
      buf.getByte(pos) == '\r' && (pos < buf.writerIndex - 1) && buf.getByte(pos + 1) == '\n'
    }
  }

  val charset = Charset.forName("UTF-8")

  var responseType: ResponseType = Unknown

  override def decode(ctx: ChannelHandlerContext, ch: Channel, buf: ChannelBuffer): AnyRef = {
    // println("decode[%s]: %h -> %s".format(Thread.currentThread.getName, this, responseType))

    responseType match {
      case Unknown if buf.readable =>
        responseType = ResponseType(buf.readByte)
        decode(ctx, ch, buf)

      case Unknown if !buf.readable => null // need more data

      case BulkData => readAsciiLine(buf) match {
        case null => null // need more data
        case line => line.toInt match {
          case -1 =>
            responseType = Unknown
            NullData
          case n =>
            responseType = BinaryData(n)
            decode(ctx, ch, buf)
        }
      }

      case BinaryData(len) =>
        if (buf.readableBytes >= (len + 2)) { // +2 for eol
            responseType = Unknown
            val data = buf.readSlice(len)
            buf.skipBytes(2) // eol is there too
            data
        } else {
            null // need more data
        }

      case x => readAsciiLine(buf) match {
        case null => null // need more data
        case line =>
          responseType = Unknown
          (x, line)
      }
    }
  }

  private def readAsciiLine(buf: ChannelBuffer): String = if (!buf.readable) null else {
    buf.indexOf(buf.readerIndex, buf.writerIndex, EOL_FINDER) match {
      case -1 => null
      case n =>
        val line = buf.toString(buf.readerIndex, n-buf.readerIndex, charset)
        buf.skipBytes(line.length + 2)
        line
    }
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) {
    handleException(ctx: ChannelHandlerContext, e: ExceptionEvent)
  }
}

private[redis] class RedisResponseAccumulator(connStateRef: AtomicReference[ConnectionState]) extends SimpleChannelHandler with ChannelExceptionHandler {
  import scala.collection.mutable.ArrayBuffer

  val bulkDataBuffer = ArrayBuffer[BulkDataResult]()
  var numDataChunks = 0

  final val BULK_NONE = BulkDataResult(None)
  final val EMPTY_MULTIBULK = MultiBulkDataResult(Seq())

  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
    e.getMessage match {
      case (resType:ResponseType, line:String) =>
        clear()
        resType match {
          case Error => handleResult(ErrorResult(line))
          case SingleLine => handleResult(SingleLineResult(line))
          case Integer => handleResult(BulkDataResult(Some(line.getBytes)))
          case MultiBulkData => line.toInt match {
            case x if x <= 0 => handleResult(EMPTY_MULTIBULK)
            case n => numDataChunks = line.toInt // ask for bulk data chunks
          }
          case _ => throw new Exception("Unexpected %s -> %s".format(resType, line))
        }
      case data: ChannelBuffer => handleDataChunk(data)
      case NullData => handleDataChunk(null)
      case _ => throw new Exception("Unexpected error: " + e.getMessage)
    }
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) {
    handleException(ctx: ChannelHandlerContext, e: ExceptionEvent)
  }

  private def handleDataChunk(bulkData: ChannelBuffer) {
    val chunk = bulkData match {
      case null =>
        BULK_NONE
      case buf =>
        if(buf.readable) {
          val bytes = new Array[Byte](buf.readableBytes())
          buf.readBytes(bytes)
          BulkDataResult(Some(bytes))
        } else BulkDataResult(None)
    }

    numDataChunks match {
      case 0 =>
        handleResult(chunk)

      case 1 =>
        bulkDataBuffer += chunk
        val allChunks = new Array[BulkDataResult](bulkDataBuffer.length)
        bulkDataBuffer.copyToArray(allChunks)
        clear()
        handleResult(MultiBulkDataResult(allChunks))

      case _ =>
        bulkDataBuffer += chunk
        numDataChunks  = numDataChunks - 1
    }
  }

  private def handleResult(r: Result) {
    try {
      val nextStateOpt = connStateRef.get().handle(r)

      for(nextState <- nextStateOpt) {
        connStateRef.set(nextState)
      }
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }

  private def clear() {
    numDataChunks = 0
    bulkDataBuffer.clear()
  }
}

/**
 * Connection can operate in two states: Normal which is used for all major commands and
 * Subscribed with reduced commands set and receiving responses without issuing commands.
 *
 * Base implementation adds support for complex commands which are commands that receives
 * more than one response. E.g. Subscribe/Unsibscribe commands receive separate BulkDataResult
 * for each specified channel, all other commands has one to one relationship with responses.
 */
sealed abstract class ConnectionState(queue: BlockingQueue[ResultFuture]) {

  var currentComplexResponse: Option[ResultFuture] = None

  def fillResult(r: Result): ResultFuture = {
    val nextFuture = nextResultFuture()

    nextFuture.fillWithResult(r)

    if(!nextFuture.complete) {
      currentComplexResponse = Some(nextFuture)
    } else {
      currentComplexResponse = None
    }

    nextFuture
  }

  def fillError(err: ErrorResult) {
    nextResultFuture().fillWithFailure(err)
    currentComplexResponse = None
  }

  /**
   * Handles results got from socket. Optionally can return new connection state.
   * @param r result to handle
   * @return new connection state or none if current state remains.
   */
  def handle(r: Result): Option[ConnectionState]

  private[this] def nextResultFuture() = currentComplexResponse getOrElse queue.poll(60, TimeUnit.SECONDS)
}

/**
 * Processes responses for all commands and completes promises of results in listeners.
 * Can be changed to subscribed state when receives results of Subscribe command.
 *
 * @param queue queue of result promises holders.
 */
case class NormalConnectionState(queue: BlockingQueue[ResultFuture]) extends ConnectionState(queue) {
  def handle(r: Result): Option[ConnectionState] = r match {
    case err: ErrorResult =>
      fillError(err)
      None
    case r: Result =>
      val respFuture = fillResult(r)

      respFuture.cmd match {
        case subscribeCmd: Subscribe if respFuture.complete =>
          Some(SubscribedConnectionState(queue, subscribeCmd))
        case _ =>
          None
      }
  }
}

/**
 * Connection state that supports only limited set of commands (Subscribe/Unsubscribe) and can process
 * messages from subscribed channels and pass them to channel subscribers.
 * When no subscribers left (as result of unsubscribing) switches to Normal connection state.
 *
 * @param queue commands queue to process
 * @param subscribe subscribe command that caused state change.
 */
case class SubscribedConnectionState(queue: BlockingQueue[ResultFuture], subscribe: Subscribe) extends ConnectionState(queue) {

  type Subscriber = MultiBulkDataResult => Unit

  var subscribers = extractSubscribers(subscribe)

  def handle(r: Result): Option[ConnectionState] = {
    r match {
      case err: ErrorResult =>
        fillError(err)
      case cmdResult: BulkDataResult =>
        val respFuture = fillResult(r)

        if(respFuture.complete) {
          respFuture.cmd match {
            case subscribeCmd: Subscribe =>
              subscribers ++= extractSubscribers(subscribeCmd)
            case unsubscribeCmd: Unsubscribe =>
              return processUnsubscribe(unsubscribeCmd.channels)
            case other =>
              new RuntimeException("Unsupported response from server in subscribed mode: " + other)
          }
        }

      case message: MultiBulkDataResult =>
        val channel = message.results(1).data.map(new String(_)).get

        subscribers.foreach { case (pattern, handler) =>
          if(channel == pattern) handler(message)
        }
      case other =>
        new RuntimeException("Unsupported response from server in subscribed mode: " + other)
    }

    None
  }

  private def processUnsubscribe(channels: Seq[String]) = {
    subscribers = subscribers.filterNot { case (channel, _) =>
      channels.contains(channel)
    }

    if(subscribers.isEmpty) {
      Some(NormalConnectionState(queue))
    } else {
      None
    }
  }

  private def extractSubscribers(cmd: Subscribe) =
    cmd.channels.map(p => (p, cmd.handler))

}