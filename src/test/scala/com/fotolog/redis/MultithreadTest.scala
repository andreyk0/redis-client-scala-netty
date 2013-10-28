package com.fotolog.redis

import org.junit.Test
import java.util.concurrent.Executors
import junit.framework.TestCase


/**
 * Created with IntelliJ IDEA.
 * User: sergey
 * Date: 10/28/13
 * Time: 3:11 PM
 * To change this template use File | Settings | File Templates.
 */

object MultithreadTest {


  def main(args: Array[String]) {

    println("Test started")

    val client = RedisClient()
    val executor = Executors.newFixedThreadPool(30)
    val times = 10000

    for(i <- 1 to 40) {
      executor.execute(new Runnable {
        def run() {
          try {

            val tid = "" + Thread.currentThread().getId + "_"
            println("start_" + tid)

            val start = System.currentTimeMillis()

            for(i <- 0 to times) {
              try{
                client.set(tid + i, "" + i)
              } catch {
                case e: Exception => e.printStackTrace
              }

            }

            val setEnd = System.currentTimeMillis()
            println("set end_" + tid)

            for(i <- 1 to times) {
              client.get[String](tid + i) match {
                case None => throw new RuntimeException("Key: " + tid + i + " is not found")
                case Some(s) => if(!s.equals("" + i)) throw new RuntimeException("Key value is wrong: " + s + ", has to be: " + i)
              }
            }

            val getEnd = System.currentTimeMillis()

            for(i <- 1 to times) {
              val s = client.del(tid + i)
            }

            val delEnd = System.currentTimeMillis()

            println("TID: " + tid + " Timing: " +
              "\tSet: " + (setEnd - start) + " rate: " + (times.asInstanceOf[Double] / (setEnd - start)) + "\n" +
              "\tGet: " + (getEnd - setEnd) + " rate: " + (times.asInstanceOf[Double] / (getEnd - setEnd)) + "\n" +
              "\tDel: " + (delEnd - getEnd) + " rate: " + (times.asInstanceOf[Double] / (delEnd - getEnd)) + "\n"
            )
          } catch {
            case e: Exception => e.printStackTrace()
          }
        }
      })
    }

    executor.shutdown()
  }
}
