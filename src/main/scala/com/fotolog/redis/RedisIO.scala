package com.fotolog.redis


import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory
import org.jboss.netty.channel._
import org.jboss.netty.buffer._
import org.jboss.netty.bootstrap.ClientBootstrap
import org.jboss.netty.handler.codec.frame.FrameDecoder
import java.net.InetSocketAddress
import org.apache.log4j.Logger
import scala.concurrent.{Await, Future, Promise}
import java.util.concurrent.{TimeUnit, Executors, ArrayBlockingQueue}
import scala.concurrent.duration.Duration
import org.jboss.netty.channel.ChannelHandler.Sharable
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder
import java.nio.charset.Charset

import RedisClientTypes._

sealed abstract class Result
case class ErrorResult(err: String) extends Result
case class SingleLineResult(msg: String) extends Result
case class IntegerResult(n: Int) extends Result
case class BulkDataResult(data: Option[BinVal]) extends Result {
    override def toString = {
        "BulkDataResult(%s)".format({ data match { case Some(barr) => new String(barr); case None => "" } })
    }
}
case class MultiBulkDataResult(results: Seq[BulkDataResult]) extends Result

case class ResultFuture(cmd: Cmd) {
  val promise = Promise[Result]()
  def future = promise.future
}


object RedisConnection {
  private[redis] type OpQueue = ArrayBlockingQueue[ResultFuture]

  private[redis] val log = Logger.getLogger(getClass)
  private[redis] val executor = Executors.newCachedThreadPool()
  private[redis] val channelFactory = new NioClientSocketChannelFactory(executor, executor)
  private[redis] val commandEncoder = new RedisCommandEncoder() // stateless
  private[redis] val cmdQueue = new ArrayBlockingQueue[Pair[RedisConnection, ResultFuture]](2048)

  scala.actors.Actor.actor { 
    while(true) {
      val (conn, f) = cmdQueue.take()
      try {
        if (conn.isOpen) {
          conn.enqueue(f)
        } else {
          log.error("Skipping cmd queued up into a closed channel (%s)".format(f.cmd))
          f.promise.failure(new IllegalStateException("Channel closed"))
        }
      } catch {
        case e: Exception => f.promise.failure(e); conn.shutdown()
      }
    }
  }
}

class RedisConnection(val host: String = "localhost", val port: Int = 6379) {

  import RedisConnection._

  private[RedisConnection] var isRunning = true
  private[RedisConnection] val clientBootstrap = new ClientBootstrap(channelFactory)
  private[RedisConnection] val opQueue = new OpQueue(128)

  clientBootstrap.setPipelineFactory(new ChannelPipelineFactory() {
      override def getPipeline = {
          val p = Channels.pipeline
          p.addLast("response_decoder",     new RedisResponseDecoder())
          p.addLast("response_accumulator", new RedisResponseAccumulator(opQueue))
          p.addLast("command_encoder",      commandEncoder)
          p
      }
  })

  clientBootstrap.setOption("tcpNoDelay", true)
  clientBootstrap.setOption("keepAlive", true)
  clientBootstrap.setOption("connectTimeoutMillis", 1000)

  private[RedisConnection] val channel = {
      val future = clientBootstrap.connect(new InetSocketAddress(host, port))
      future.await(1, TimeUnit.MINUTES)
      if (future.isSuccess) {
        future.getChannel
      } else {
        throw future.getCause
      }
  }

  log.info("Connecting to %s:%s".format(host,port))
  forceChannelOpen()


  def send(cmd: Cmd): Future[Result] = {
    val f = new ResultFuture(cmd)
    cmdQueue.offer((this, f), 10, TimeUnit.SECONDS)
    f.future
  }

  def enqueue(f: ResultFuture) {
    opQueue.offer(f, 10, TimeUnit.SECONDS)
    channel.write(f).addListener(ChannelFutureListener.CLOSE_ON_FAILURE)
  }

  def isOpen: Boolean = isRunning && channel.isOpen

  def shutdown() {
    try {
      isRunning = false
      channel.close().await(1, TimeUnit.MINUTES)
    } catch {
      case e: Exception => log.error(e.getMessage, e)
    }
  }

  private def forceChannelOpen() {
    val f = new ResultFuture(Ping())
    enqueue(f)
    Await.result(f.future, Duration(1, TimeUnit.MINUTES))
  }
}

private[redis] object RedisCommandEncoder {
  val SPACE = " ".getBytes
  val EOL = "\r\n".getBytes

  val DEL = "DEL".getBytes
  val GET = "GET".getBytes
  val MGET = "MGET".getBytes
  val SET = "SET".getBytes
  val MSET = "MSET".getBytes
  val GETSET = "GETSET".getBytes
  val SETNX = "SETNX".getBytes
  val MSETNX = "MSETNX".getBytes
  val SETEX = "SETEX".getBytes
  val INCR = "INCR".getBytes
  val INCRBY = "INCRBY".getBytes
  val DECR = "DECR".getBytes
  val DECRBY = "DECRBY".getBytes
  val APPEND = "APPEND".getBytes
  val SUBSTR = "SUBSTR".getBytes
  val EXPIRE = "EXPIRE".getBytes
  val PERSIST = "PERSIST".getBytes
  val RPUSH = "RPUSH".getBytes
  val LPUSH = "LPUSH".getBytes
  val LLEN = "LLEN".getBytes
  val LRANGE = "LRANGE".getBytes
  val LTRIM = "LTRIM".getBytes
  val LINDEX = "LINDEX".getBytes
  val LSET = "LSET".getBytes
  val LREM = "LREM".getBytes
  val LPOP = "LPOP".getBytes
  val RPOP = "RPOP".getBytes
  val BLPOP = "BLPOP".getBytes
  val BRPOP = "BRPOP".getBytes
  val RPOPLPUSH = "RPOPLPUSH".getBytes
  val HSET = "HSET".getBytes
  val HGET = "HGET".getBytes
  val HMGET = "HMGET".getBytes
  val HMSET = "HMSET".getBytes
  val HINCRBY = "HINCRBY".getBytes
  val HEXISTS    = "HEXISTS".getBytes
  val HDEL = "HDEL".getBytes
  val HLEN = "HLEN".getBytes
  val HKEYS = "HKEYS".getBytes
  val HVALS = "HVALS".getBytes
  val HGETALL = "HGETALL".getBytes
  val SADD = "SADD".getBytes
  val SREM = "SREM".getBytes
  val SPOP = "SPOP".getBytes
  val SMOVE = "SMOVE".getBytes
  val SCARD = "SCARD".getBytes
  val SISMEMBER = "SISMEMBER".getBytes
  val SINTER = "SINTER".getBytes
  val SINTERSTORE = "SINTERSTORE".getBytes
  val SUNION = "SUNION".getBytes
  val SUNIONSTORE = "SUNIONSTORE".getBytes
  val SDIFF = "SDIFF".getBytes
  val SDIFFSTORE = "SDIFFSTORE".getBytes
  val SMEMBERS = "SMEMBERS".getBytes
  val SRANDMEMBER = "SRANDMEMBER".getBytes
  val SORT = "SORT".getBytes
  val PING = "PING".getBytes
  val EXISTS = "EXISTS".getBytes
  val TYPE = "TYPE".getBytes
  val INFO = "INFO".getBytes
  val FLUSHALL = "FLUSHALL".getBytes
}

@Sharable
private[redis] class RedisCommandEncoder extends OneToOneEncoder {
  import org.jboss.netty.buffer.ChannelBuffers._
  import RedisCommandEncoder._

  override def encode(ctx: ChannelHandlerContext, channel: Channel, msg: AnyRef): AnyRef = {
    //println("encode[%s]: %h -> %s".format(Thread.currentThread.getName, this, msg))
    val opFuture = msg.asInstanceOf[ResultFuture]
    toChannelBuffer(opFuture.cmd)
  }

  private def toChannelBuffer(cmd: Cmd): ChannelBuffer = cmd match {
    case Del(key) => copiedBuffer(DEL, SPACE, key.getBytes, EOL)
    case Del(keys @ _*) => multiKeyCmd(DEL, keys)
    case Get(key) => copiedBuffer(GET, SPACE, key.getBytes, EOL)
    case MGet(keys @ _*) => multiKeyCmd(MGET, keys)
    case Set(key, value) => binaryCmd(SET, key.getBytes, value)
    case mset: MSet => binarySetCmd(MSET, mset.kvs :_*)
    case GetSet((key, value)) => binaryCmd(GETSET, key.getBytes, value)
    case SetNx((key, value)) => binaryCmd(SETNX, key.getBytes, value)
    case setNxMulti: SetNx => binarySetCmd(MSETNX, setNxMulti.kvs: _*)
    case SetEx(key, expTime, value) => binaryCmd(SETEX, key.getBytes, expTime.toString.getBytes, value)
    case Incr(key, 1) => copiedBuffer(INCR, SPACE, key.getBytes, EOL)
    case Incr(key, delta) => copiedBuffer(INCRBY, SPACE, key.getBytes, SPACE, delta.toString.getBytes, EOL)
    case Decr(key, 1) => copiedBuffer(DECR, SPACE, key.getBytes, EOL)
    case Decr(key, delta) => copiedBuffer(DECRBY, SPACE, key.getBytes, SPACE, delta.toString.getBytes, EOL)
    case Append((key, value)) => binaryCmd(APPEND, key.getBytes, value)
    case Substr(key, startOffset, endOffset) => copiedBuffer(SUBSTR, SPACE, key.getBytes, SPACE, startOffset.toString.getBytes, SPACE, endOffset.toString.getBytes, EOL)
    case Persist(key) => copiedBuffer(PERSIST, SPACE, key.getBytes, EOL)
    case Expire(key, seconds) => copiedBuffer(EXPIRE, SPACE, key.getBytes, SPACE, seconds.toString.getBytes, EOL)
    case Rpush((key, value)) => binaryCmd(RPUSH, key.getBytes, value)
    case Lpush((key, value)) => binaryCmd(LPUSH, key.getBytes, value)
    case Llen(key) => copiedBuffer(LLEN, SPACE, key.getBytes, EOL)
    case Lrange(key, start, end) => copiedBuffer(LRANGE, SPACE, key.getBytes, SPACE, start.toString.getBytes, SPACE, end.toString.getBytes, EOL)
    case Ltrim(key, start, end) => copiedBuffer(LTRIM, SPACE, key.getBytes, SPACE, start.toString.getBytes, SPACE, end.toString.getBytes, EOL)
    case Lindex(key, idx) => copiedBuffer(LINDEX, SPACE, key.getBytes, SPACE, idx.toString.getBytes, EOL)
    case Lset(key, idx, value) => binaryCmd(LSET, key.getBytes, idx.toString.getBytes, value)
    case Lrem(key, count, value) => binaryCmd(LREM, key.getBytes, count.toString.getBytes, value)
    case Lpop(key) => copiedBuffer(LPOP, SPACE, key.getBytes, EOL)
    case Rpop(key) => copiedBuffer(RPOP, SPACE, key.getBytes, EOL)
    case RpopLpush(srcKey, destKey) => copiedBuffer(RPOPLPUSH, SPACE, srcKey.getBytes, SPACE, destKey.getBytes, EOL)
    case Hset(key, field, value) => binaryCmd(HSET, key.getBytes, field.getBytes, value)
    case Hget(key, field) => binaryCmd(HGET, key.getBytes, field.getBytes)
    case Hmget(key, fields @ _*) => binaryCmd(HMGET :: key.getBytes :: fields.toList.map{_.getBytes}: _*)
    case hmSet: Hmset => binaryHmSetCmd(hmSet)
    case Hincrby(key, field, delta) => binaryCmd(HINCRBY, key.getBytes, field.getBytes, delta.toString.getBytes)
    case Hexists(key, field) => binaryCmd(HEXISTS, key.getBytes, field.getBytes)
    case Hdel(key, field) => binaryCmd(HDEL, key.getBytes, field.getBytes)
    case Hlen(key) => binaryCmd(HLEN, key.getBytes)
    case Hkeys(key) => binaryCmd(HKEYS, key.getBytes)
    case Hvals(key) => binaryCmd(HVALS, key.getBytes)
    case Hgetall(key) => binaryCmd(HGETALL, key.getBytes)
    case Sadd((key, value)) => binaryCmd(SADD, key.getBytes, value)
    case Srem((key, value)) => binaryCmd(SREM, key.getBytes, value)
    case Spop(key) => copiedBuffer(SPOP, SPACE, key.getBytes, EOL)
    case Smove(srcKey, destKey, value) => binaryCmd(SMOVE, srcKey.getBytes, destKey.getBytes, value)
    case Scard(key) => copiedBuffer(SCARD, SPACE, key.getBytes, EOL)
    case Sismember((key, value)) => binaryCmd(SISMEMBER, key.getBytes, value)
    case Sinter(keys @ _*) => binaryCmd(SINTER :: keys.toList.map{_.getBytes}: _*)
    case Sinterstore(destKey, keys @ _*) => binaryCmd(SINTERSTORE :: destKey.getBytes :: keys.toList.map{_.getBytes}: _*)
    case Sunion(keys @ _*) => binaryCmd(SUNION:: keys.toList.map{_.getBytes}: _*)
    case Sunionstore(destKey, keys @ _*) => binaryCmd(SUNIONSTORE:: destKey.getBytes :: keys.toList.map{_.getBytes}: _*)
    case Sdiff(keys @ _*) => binaryCmd(SDIFF :: keys.toList.map{_.getBytes}: _*)
    case Sdiffstore(destKey, keys @ _*) => binaryCmd(SDIFFSTORE :: destKey.getBytes :: keys.toList.map{_.getBytes}: _*)
    case Smembers(key) => copiedBuffer(SMEMBERS, SPACE, key.getBytes, EOL)
    case Srandmember(key) => copiedBuffer(SRANDMEMBER, SPACE, key.getBytes, EOL)
    case Ping() => copiedBuffer(PING, EOL)
    case Exists(key) => copiedBuffer(EXISTS, SPACE, key.getBytes, EOL)
    case Type(key) => copiedBuffer(TYPE, SPACE, key.getBytes, EOL)
    case Info() => copiedBuffer(INFO, EOL)
    case FlushAll() => copiedBuffer(FLUSHALL, EOL)
  }

  private def multiKeyCmd(cmd: BinVal, keys: Seq[String]): ChannelBuffer = {
    val params = new Array[BinVal](2*keys.length + 2)
    params(0) = cmd
    var i=1
    for(k <- keys) {
        params(i) = SPACE       ; i = i+1
        params(i) = k.getBytes  ; i = i+1
    }
    params(params.length-1) = EOL
    copiedBuffer(params: _*)
  }

  private def binaryHmSetCmd(hmSet: Hmset): ChannelBuffer = {
    binaryCmd(HMSET :: hmSet.key.getBytes :: hmSet.kvs.toList.map{kv => List(kv._1.getBytes, kv._2)}.flatten: _*)
  }

  private def binarySetCmd(cmd: BinVal, kvs: KV*): ChannelBuffer = {
    binaryCmd(cmd :: kvs.toList.map{kv => List(kv._1.getBytes, kv._2)}.flatten: _*)
  }

  private def binaryCmd(cmdParts: BinVal*): ChannelBuffer = {
    val params = new Array[BinVal](3*cmdParts.length + 1)
    params(0) = ("*" + cmdParts.length + "\r\n").getBytes // num binary chunks
    var i=1
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
    RedisConnection.log.error(e.getCause.getMessage, e.getCause)
    e.getChannel.close() // don't allow any more ops on this channel, pipeline is busted
  }
}

private[redis] class RedisResponseDecoder extends FrameDecoder with ChannelExceptionHandler {

  val EOL_FINDER = new ChannelBufferIndexFinder() {
    override def find(buf: ChannelBuffer, pos: Int): Boolean = {
      buf.getByte(pos) == '\r' && (pos < buf.writerIndex - 1) && buf.getByte(pos + 1) == '\n'
    }
  }

  val ASCII = Charset.forName("US-ASCII")

  var responseType: ResponseType = Unknown

  override def decode(ctx: ChannelHandlerContext, ch: Channel, buf: ChannelBuffer): AnyRef = {
    // println("decode[%s]: %h -> %s".format(Thread.currentThread.getName, this, responseType))

    responseType match {
      case Unknown if buf.readable => {
          responseType = ResponseType(buf.readByte)
          decode(ctx, ch, buf)
      }

      case Unknown if !buf.readable => null // need more data

      case BulkData => readAsciiLine(buf) match {
          case null => null // need more data
          case line => line.toInt match {
              case -1 => {
                  responseType = Unknown
                  NullData
              }
              case n => {
                  responseType = BinaryData(n)
                  decode(ctx, ch, buf)
              }
          }
      }

      case BinaryData(len) => {
          if (buf.readableBytes >= (len + 2)) { // +2 for eol
              responseType = Unknown
              val data = buf.readSlice(len)
              buf.skipBytes(2) // eol is there too
              data
          } else {
              null // need more data
          }
      }

      case x => readAsciiLine(buf) match {
          case null => null // need more data
          case line => {
              responseType = Unknown
              (x, line)
          }
      }
    }
  }

  private def readAsciiLine(buf: ChannelBuffer): String = if (!buf.readable) null else {
    buf.indexOf(buf.readerIndex, buf.writerIndex, EOL_FINDER) match {
      case -1 => null
      case n => {
        val line = buf.toString(buf.readerIndex, n-buf.readerIndex, ASCII)
        buf.skipBytes(line.length + 2)
        line
      }
    }
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) {
    handleException(ctx: ChannelHandlerContext, e: ExceptionEvent)
  }
}

private[redis] class RedisResponseAccumulator(opQueue: RedisConnection.OpQueue) extends SimpleChannelHandler with ChannelExceptionHandler {
  import scala.collection.mutable.ArrayBuffer

  val bulkDataBuffer = ArrayBuffer[BulkDataResult]()
  var numDataChunks = 0

  final val BULK_NONE = BulkDataResult(None)
  final val EMPTY_MULTIBULK = MultiBulkDataResult(Seq())

  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
    // println("accum[%s]: %h -> %s".format(Thread.currentThread.getName, this, e.getMessage))

    e.getMessage match {
      case (resType:ResponseType, line:String) => {
        clear()
        resType match {
          case Error => success(ErrorResult(line))
          case SingleLine => success(SingleLineResult(line))
          case Integer => success(IntegerResult(line.toInt))
          case MultiBulkData => line.toInt match {
              case x if x <= 0 => success(EMPTY_MULTIBULK)
              case n => numDataChunks = line.toInt // ask for bulk data chunks
          }
          case _ => throw new Exception("Unexpected %s -> %s".format(resType, line))
        }
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
      case null => BULK_NONE
      case buf => {
        val bytes = new BinVal(buf.readableBytes())
        buf.readBytes(bytes)
        BulkDataResult(Some(bytes))
      }
    }

    numDataChunks match {
      case 0 => success(chunk)
      case 1 => {
        bulkDataBuffer += chunk
        val allChunks = new Array[BulkDataResult](bulkDataBuffer.length)
        bulkDataBuffer.copyToArray(allChunks)
        clear()
        success(MultiBulkDataResult(allChunks))
      }
      case _ => {
        bulkDataBuffer += chunk
        numDataChunks  = numDataChunks - 1
      }
    }
  }

  private def success(r: Result) {
    val respFuture = opQueue.poll(60, TimeUnit.SECONDS)
    respFuture.promise.success(r)
  }

  private def clear() {
      numDataChunks = 0
      bulkDataBuffer.clear()
  }
}
