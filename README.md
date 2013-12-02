## [Redis](http://redis.io) scala client library

Forked from [andreyk0](https://github.com/andreyk0/redis-client-scala-netty).

Changes from original one:
* added support of [scripting](http://redis.io/commands#scripting) commands
* moved to scala [futures](http://docs.scala-lang.org/overviews/core/futures.html)
* binary safe requests encoding
* added maven support

## Why yet another scala client?
* binary safe values
* asynchronous calls
* protocol pipelining
* connection multiplexing

## Building by [buildr tool](http://buildr.apache.org/):
    $ buildr package

## Building by [maven](http://maven.apache.org/):
    $ mvn package

Unit tests assume redis is running on localhost on port 6380, THEY WILL FLUSH ALL DATA!
To quickly build/package without running the tests:

    $ buildr package test=no

of for maven:

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
    
 
