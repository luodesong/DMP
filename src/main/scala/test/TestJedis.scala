package test

import com.luodesong.util.JedisPool
import redis.clients.jedis.Jedis


object TestJedis {
    def main(args: Array[String]): Unit = {
        val cluster: Jedis  = JedisPool.getMyrdis()
        val str: String = cluster.get("test1")
        println(str)
    }
}
