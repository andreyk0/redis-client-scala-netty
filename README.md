## [Redis](http://code.google.com/p/redis/) scala client library
We are working on this code @fotolog.com, decided to open it up in case somebody else finds it useful.

Tested with Redis version 2.0.1, some commands won't work with older version

## Why yet another scala client?
* binary safe values
* asynchronous calls
* protocol pipelining
* connection multiplexing


## To build (requires buildr tool from http://buildr.apache.org/):
    $ buildr package

Ruby gem system is a bit quirky on ubuntu 10.04, so, download buildr-1.4.1.gem from [rubygems](http://rubygems.org/) and  

    $ export JAVA_HOME=/path/to/your/java
    $ sudo gem install buildr-1.4.1.gem
    $ export PATH=/var/lib/gems/1.8/bin:$PATH

Unit tests assume redis is running on localhost on port 6380, THEY WILL FLUSH ALL DATA!
To quickly build/package without running the tests:

    $ buildr package test=no


## Example usage with scala REPL
    $ buildr shell

    import com.fotolog.redis.{RedisCluster, RedisClient, RedisHost, Conversions}
    import Conversions._
    
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
    // .... do something else
    future.get
    
    // explicit encoding/decoding of binary values
    r.set("boo", "foo") { x: String =>
        x.getBytes("UTF-8")
    }
    r.get("boo") { x: Array[Byte] =>
        new String(x, "UTF-8")
    }


## Example of adding other conversion functions

   import com.google.protobuf.{CodedOutputStream,CodedInputStream}

    implicit def convertIntToByteArray(i: Int): Array[Byte] = {
        val barr = new Array[Byte](CodedOutputStream.computeInt32SizeNoTag(i))
        val os = CodedOutputStream.newInstance(barr)
        os.writeInt32NoTag(i)
        barr
    }
    implicit def convertByteArrayToInt(b: Array[Byte]): Int = {
        val is = CodedInputStream.newInstance(b)
        is.readInt32()
    }
    implicit def convertLongToByteArray(i: Long): Array[Byte] = {
        val barr = new Array[Byte](CodedOutputStream.computeInt64SizeNoTag(i))
        val os = CodedOutputStream.newInstance(barr)
        os.writeInt64NoTag(i)
        barr
    }
    implicit def convertByteArrayToLong(b: Array[Byte]): Long = {
        val is = CodedInputStream.newInstance(b)
        is.readInt64()
    }

    import com.google.protobuf.Message
    implicit def convertProtobufToByteArray(m: Message): Array[Byte] = m.toByteArray
    // to convert back from protobuf pass { YourProto.parseFrom } as a conversion function
