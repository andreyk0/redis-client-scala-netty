## [Redis](http://redis.io) scala client library

## Features

* binary safe values
* really asynchronous calls without any thread pools
* protocol pipelining
* connection multiplexing
* in-memory redis implementation which can come in handy for testing (in active development)

Changes from [original version](https://github.com/andreyk0/redis-client-scala-netty):
* added support of [scripting](http://redis.io/commands#scripting) commands
* moved to scala [futures](http://docs.scala-lang.org/overviews/core/futures.html)
* binary safe requests encoding
* moved to maven
* in-memory client

## Building by [maven](http://maven.apache.org/):
    $ mvn package

Unit tests assume redis is running on localhost on port 6380, THEY WILL FLUSH ALL DATA!

    $ mvn package -DskipTests=true

## Example usage with scala REPL

    import com.fotolog.redis.{RedisCluster, RedisClient, RedisHost}

    val shardingHashFun = (s:String)=>s.hashCode // shard on string values
    val cluster = new RedisCluster[String](shardingHashFun, RedisHost("localhost", 6379) /*, more redis hosts */)
    
    val r = cluster("egusername") // get redis client
    
    // basic
    
    r.set("foo", "bar")
    r.get("foo") // byte arrays by default
    r.get[String]("foo") // typed, hints at implicit conversion
    
    // lists
    r.lpush("ll", "l1")
    r.lpush("ll", "l2")
    r.lrange[String]("ll", 0, 100)
    r.lpop[String]("ll")
    
    // sets
    r.sadd("ss", "foo")
    r.sadd("ss", "bar")
    r.smembers[String]("ss")
    
    // async calls
    val future = r.lpopAsync[String]("ll") // prefetch data
    future.map(_.toLong) // map to another value
    
    // Do something else 
    
    import scala.concurrent.Await
    import scala.concurrent.duration.Duration
    import java.util.concurrent.TimeUnit
    
    val res = Await.result(future, Duration(1, TimeUnit.MINUTES)) // wait for result for 1 min.

    // dealing with Lua scripts:
    
    val resultsList = c.eval[Int]("return ARGV[1];", ("anyKey", "2"))
    val scriptHash = c.scriptLoad("return ARGV[1];")
    val evalResultsList = c.evalsha[Int](scriptHash, ("key", "4"))


## Example of adding custom conversion objects
 
 Conversion is implemented using BinaryConverter interface.

    import com.google.protobuf.{CodedOutputStream,CodedInputStream}

    implicit val intProtobufConverter = new BinaryConverter[Int]() {
        def read(data: BinVal) = {
            val is = CodedInputStream.newInstance(b)
            is.readInt32()    
        }
        
        def write(v: Int) = {
            val barr = new Array[Byte](CodedOutputStream.computeInt32SizeNoTag(i))
            val os = CodedOutputStream.newInstance(barr)
            os.writeInt32NoTag(i)
            barr
        }
    }


Conversion objects can be passed explicitly:
 
    c.set("key-name", 15)(intProtobufConverter)
    
 
## In-memory client usage

It is not necessary now to run standalone redis server for developing or unit testing, simply changing host name can force
client to do perform all operations in memory. Also it is possible to emulate several databases at once. The behaviour is
similar to in-memory databases in popular embeddable RDMS like H2 or HyperSQL. Following creates in-memory database `test`:

    val c = RedisClient("mem:test")

Note: feature is in active development so only basic operations are supported now.