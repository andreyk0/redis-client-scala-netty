## [Redis](http://redis.io) scala client library [![Build Status](https://travis-ci.org/sergkh/redis-client-scala-netty.svg?branch=master)](https://travis-ci.org/sergkh/redis-client-scala-netty)

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
* Distributed lock implementation ([Redlock](http://redis.io/topics/distlock))

## Building by [maven](http://maven.apache.org/):
    $ mvn package

Unit tests assume redis is running on localhost on port 6380, THEY WILL FLUSH ALL DATA!

    $ mvn package -DskipTests=true

## Example usage with scala REPL


```scala
    import com.fotolog.redis.RedisClient

    val r = RedisClient("localhost", 6379)

    // basic

    r.set("foo", "bar")
    r.get[String]("foo") // get method has to be typed, hints at implicit conversion

    // lists
    r.lpush("ll", "l1")
    r.lpush("ll", "l2")
    r.lrange[String]("ll", 0, 100)
    r.lpop[String]("ll")

    // sets
    r.sadd("ss", "foo")
    r.sadd("ss", "bar")
    r.smembers[String]("ss")

```

For convenience any API method has both synchronous and asynchronous versions:

```scala

    val fooFuture = r.getAsync[String]("foo")

    val future = r.lpopAsync[String]("ll") // prefetch data
    future.map(_.toLong) // map to another value

```

Synchronous methods by default wait up to 1 minute for the future to complete, but one can specify duration on client initialization:

```scala
    import scala.concurrent.duration._

    val r = RedisClient("localhost", timeout = 20 seconds)

```  

Dealing with Lua scripts:

```scala

    val resultsList = c.eval[Int]("return ARGV[1];", ("anyKey", "2"))
    val scriptHash = c.scriptLoad("return ARGV[1];")
    val evalResultsList = c.evalsha[Int](scriptHash, ("key", "4"))

```

Hyper Log Log:

```scala

    val anyAddedH1 = c.pfadd("h1", "a", "b", "c", "d") // true
    val anyAddedH2 = c.pfadd("h2", "a", "b", "f", "g") // true
    val count = c.pfcount("h1") // 4

    c.pfmerge("merge-result", "h1", "h2")
    val mergeCount = c.pfcount("merge-result") // 6

```

Sharding example:

```scala
    import com.fotolog.redis.{RedisCluster, RedisClient, RedisHost}

    val shardingHashFunc = (s:String) => s.hashCode // shard on string values
    val cluster = new RedisCluster[String](shardingHashFunc, RedisHost("localhost", 6379) /*, more redis hosts */)

    val r = cluster("egusername") // get redis client
```

## Example of adding custom conversion objects

 Conversion is implemented using BinaryConverter interface.
```scala
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
```

Conversion objects can be passed explicitly:
```scala
    c.set("key-name", 15)(intProtobufConverter)
```    

## In-memory client usage

It is not necessary now to run a standalone Redis server for development or unit tests, simply changing host name can force
client to perform all operations in memory. Also it is possible to emulate several databases at once. The behavior is
similar to in-memory databases in popular embeddable RDMS like H2 or HyperSQL. Following code creates an in-memory database `test`:

```scala
    val c = RedisClient("mem:test")
```

Note: feature is in active development so not all operations are supported now.

## Redlock usage
Redlock is a distributed lock implementation on multiple (or one) redis instances based on algorithm described  ([here](http://redis.io/topics/distlock)).
Usage example:

```scala
    val redlock = Redlock("192.168.2.11" -> 6379, "192.168.2.12" -> 6379, "192.168.2.13" -> 6379)

    val lock = redlock.lock("resource-name", ttl = 60)

    if(lock.successful) {
        /* Some code that has to be executed only on one instance/thread. */
    }
```

One have to specify some name common for all application instances. In case of success `lock` method returns object that contains resource name and some random value that should be used to unlock resource:

    redlock.unlock(lock)

Manual unlocking is not always necessary as lock will be unlocked automatically on Redis server after `ttl` seconds will pass.

Also there is convenient `withLock` method:

```scala
    val redlock = Redlock("192.168.2.15")
    redlock.withLock("resource-name") {
        /* Some code that has to be executed only on one instance/thread. */
    }
```

Redlock is fully supported by in-memory client.
