package com.fotolog.redis

import java.util.concurrent.atomic.AtomicReference

case class RedisHost(host: String, port: Int)

class RedisCluster[Shard](hash: (Shard)=>Int, hosts: RedisHost*) {
    val h2cRef = new AtomicReference[Map[RedisHost, RedisClient]](Map.empty)
    
    def apply(s: Shard): RedisClient = {
        val h = hosts(hash(s).abs % hosts.length)
        h2cRef.get.get(h) match {
            case Some(c) => if (c.isConnected) c else newClient(h)
            case None => newClient(h)
        }
    }

    private[RedisCluster] def newClient(h: RedisHost): RedisClient = h.synchronized {
        var h2c = h2cRef.get
        def newClientThreadSafe(): RedisClient = {
            val c = RedisClient(h.host, h.port)
            while(!h2cRef.compareAndSet(h2c, h2c + (h->c))) h2c = h2cRef.get
            c
        }
        h2c.get(h) match {
            case None => newClientThreadSafe()
            case Some(c) => if (c.isConnected){ c } else { c.shutdown(); newClientThreadSafe() }
        }
    }
}

