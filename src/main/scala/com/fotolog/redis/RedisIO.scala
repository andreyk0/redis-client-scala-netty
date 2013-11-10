package com.fotolog.redis


import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory
import org.jboss.netty.channel._
import org.jboss.netty.buffer._
import org.jboss.netty.bootstrap.ClientBootstrap
import org.jboss.netty.handler.codec.frame.FrameDecoder
import java.net.InetSocketAddress
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
          f.promise.failure(new IllegalStateException("Channel closed, command: " + f.cmd))
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
    isRunning = false
    channel.close().await(1, TimeUnit.MINUTES)
  }

  private def forceChannelOpen() {
    val f = new ResultFuture(Ping())
    enqueue(f)
    Await.result(f.future, Duration(1, TimeUnit.MINUTES))
  }
}

private[redis] object RedisCommandEncoder {

  val charset = Charset.forName("UTF-8")

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

  val EVAL = "EVAL".getBytes
  val EVALSHA = "EVALSHA".getBytes
  val SCRIPT_LOAD = "SCRIPT LOAD".getBytes
  val SCRIPT_FLUSH = "SCRIPT FLUSH".getBytes
  val SCRIPT_KILL = "SCRIPT KILL".getBytes
  val SCRIPT_EXISTS = "SCRIPT EXISTS".getBytes

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
    case Del(key) => binaryCmd(DEL, key.getBytes(charset))
    case Del(keys @ _*) => binaryCmd(DEL:: keys.toList.map(_.getBytes(charset)): _*)
    case Get(key) => binaryCmd(GET, key.getBytes(charset))
    case MGet(keys @ _*) => binaryCmd(MGET :: keys.toList.map(_.getBytes(charset)): _*)
    case Set(key, value) => binaryCmd(SET, key.getBytes(charset), value)
    case mset: MSet => binaryCmd(MSET :: mset.kvs.toList.map{kv => List(kv._1.getBytes(charset), kv._2)}.flatten: _*)
    case GetSet((key, value)) => binaryCmd(GETSET, key.getBytes(charset), value)
    case SetNx((key, value)) => binaryCmd(SETNX, key.getBytes(charset), value)
    case setNxMulti: SetNx => binaryCmd(MSETNX :: setNxMulti.kvs.toList.map{kv => List(kv._1.getBytes(charset), kv._2)}.flatten: _*)
    case SetEx(key, expTime, value) => binaryCmd(SETEX, key.getBytes(charset), expTime.toString.getBytes(charset), value)
    case Incr(key, 1) =>  binaryCmd(INCR, key.getBytes(charset))
    case Incr(key, delta) => binaryCmd(INCRBY, key.getBytes(charset), delta.toString.getBytes(charset))
    case Decr(key, 1) => binaryCmd(DECR, key.getBytes(charset))
    case Decr(key, delta) => binaryCmd(DECRBY, key.getBytes(charset), delta.toString.getBytes(charset))
    case Append((key, value)) => binaryCmd(APPEND, key.getBytes(charset), value)
    case Substr(key, startOffset, endOffset) => binaryCmd(SUBSTR, key.getBytes(charset), startOffset.toString.getBytes(charset), endOffset.toString.getBytes(charset))
    case Persist(key) => binaryCmd(PERSIST, key.getBytes(charset))
    case Expire(key, seconds) => binaryCmd(EXPIRE, key.getBytes(charset), seconds.toString.getBytes(charset))
    case Rpush((key, value)) => binaryCmd(RPUSH, key.getBytes(charset), value)
    case Lpush((key, value)) => binaryCmd(LPUSH, key.getBytes(charset), value)
    case Llen(key) => binaryCmd(LLEN, key.getBytes(charset))
    case Lrange(key, start, end) => binaryCmd(LRANGE, key.getBytes(charset), start.toString.getBytes(charset), end.toString.getBytes(charset))
    case Ltrim(key, start, end) => binaryCmd(LTRIM, key.getBytes(charset), start.toString.getBytes(charset), end.toString.getBytes(charset))
    case Lindex(key, idx) => binaryCmd(LINDEX, key.getBytes(charset), idx.toString.getBytes(charset))
    case Lset(key, idx, value) => binaryCmd(LSET, key.getBytes(charset), idx.toString.getBytes(charset), value)
    case Lrem(key, count, value) => binaryCmd(LREM, key.getBytes(charset), count.toString.getBytes(charset), value)
    case Lpop(key) => binaryCmd(LPOP, key.getBytes(charset))
    case Rpop(key) => binaryCmd(RPOP, key.getBytes(charset))
    case RpopLpush(srcKey, destKey) => binaryCmd(RPOPLPUSH, srcKey.getBytes(charset), destKey.getBytes(charset))
    case Hset(key, field, value) => binaryCmd(HSET, key.getBytes(charset), field.getBytes(charset), value)
    case Hget(key, field) => binaryCmd(HGET, key.getBytes(charset), field.getBytes(charset))
    case Hmget(key, fields @ _*) => binaryCmd(HMGET :: key.getBytes(charset) :: fields.toList.map{_.getBytes(charset)}: _*)
    case hmSet: Hmset => binaryCmd(HMSET :: hmSet.key.getBytes :: hmSet.kvs.toList.map{kv => List(kv._1.getBytes(charset), kv._2)}.flatten: _*)
    case Hincrby(key, field, delta) => binaryCmd(HINCRBY, key.getBytes(charset), field.getBytes(charset), delta.toString.getBytes(charset))
    case Hexists(key, field) => binaryCmd(HEXISTS, key.getBytes(charset), field.getBytes(charset))
    case Hdel(key, field) => binaryCmd(HDEL, key.getBytes(charset), field.getBytes(charset))
    case Hlen(key) => binaryCmd(HLEN, key.getBytes(charset))
    case Hkeys(key) => binaryCmd(HKEYS, key.getBytes(charset))
    case Hvals(key) => binaryCmd(HVALS, key.getBytes(charset))
    case Hgetall(key) => binaryCmd(HGETALL, key.getBytes(charset))
    case Sadd((key, value)) => binaryCmd(SADD, key.getBytes(charset), value)
    case Srem((key, value)) => binaryCmd(SREM, key.getBytes(charset), value)
    case Spop(key) => binaryCmd(SPOP, key.getBytes(charset))
    case Smove(srcKey, destKey, value) => binaryCmd(SMOVE, srcKey.getBytes(charset), destKey.getBytes(charset), value)
    case Scard(key) => binaryCmd(SCARD, key.getBytes(charset))
    case Sismember((key, value)) => binaryCmd(SISMEMBER, key.getBytes(charset), value)
    case Sinter(keys @ _*) => binaryCmd(SINTER :: keys.toList.map{_.getBytes(charset)}: _*)
    case Sinterstore(destKey, keys @ _*) => binaryCmd(SINTERSTORE :: destKey.getBytes(charset) :: keys.toList.map{_.getBytes(charset)}: _*)
    case Sunion(keys @ _*) => binaryCmd(SUNION:: keys.toList.map{_.getBytes(charset)}: _*)
    case Sunionstore(destKey, keys @ _*) => binaryCmd(SUNIONSTORE:: destKey.getBytes(charset) :: keys.toList.map{_.getBytes(charset)}: _*)
    case Sdiff(keys @ _*) => binaryCmd(SDIFF :: keys.toList.map{_.getBytes(charset)}: _*)
    case Sdiffstore(destKey, keys @ _*) => binaryCmd(SDIFFSTORE :: destKey.getBytes(charset) :: keys.toList.map{_.getBytes(charset)}: _*)
    case Smembers(key) => binaryCmd(SMEMBERS, key.getBytes(charset))
    case Srandmember(key) => binaryCmd(SRANDMEMBER, key.getBytes(charset))

    case eval: Eval => binaryCmd(EVAL :: eval.script.getBytes(charset) :: eval.kv.length.toString.getBytes :: eval.kv.toList.map{kv => List(kv._1.getBytes(charset), kv._2)}.flatten: _*)
    case evalsha: EvalSha => binaryCmd(EVALSHA :: evalsha.digest.getBytes(charset) :: evalsha.kv.length.toString.getBytes :: evalsha.kv.toList.map{kv => List(kv._1.getBytes(charset), kv._2)}.flatten: _*)

    case ScriptLoad(script) => binaryCmd(SCRIPT_LOAD, script.getBytes(charset))
    case ScriptKill() => binaryCmd(SCRIPT_KILL)
    case ScriptFlush() => binaryCmd(SCRIPT_KILL)
    case exists: ScriptExists => binaryCmd(SCRIPT_EXISTS, exists.script.getBytes(charset))

    case Ping() => binaryCmd(PING)
    case Exists(key) => binaryCmd(EXISTS, key.getBytes(charset))
    case Type(key) => binaryCmd(TYPE, key.getBytes(charset))
    case Info() => binaryCmd(INFO)
    case FlushAll() => binaryCmd(FLUSHALL)
  }

  private def binaryCmd(cmdParts: BinVal*): ChannelBuffer = {
    val params = new Array[BinVal](3*cmdParts.length + 1)
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
        val line = buf.toString(buf.readerIndex, n-buf.readerIndex, charset)
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
          case Integer => success(BulkDataResult(Some(line.getBytes)))
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
        if(buf.readable) {
          val bytes = new BinVal(buf.readableBytes())
          buf.readBytes(bytes)
          BulkDataResult(Some(bytes))
        } else BulkDataResult(None)
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
