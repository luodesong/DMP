import com.luodesong.tag.TagBusi
import com.luodesong.util.JedisPool
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test
import redis.clients.jedis.Jedis

class TestTagBus {
    @Test
    def testOne(): Unit ={

        val conf = new SparkConf()
                .setAppName(this.getClass.getName).setMaster("local[*]")
                // 处理数据，采取scala的序列化方式，性能比Java默认的高
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        val sc = new SparkContext(conf)

        val sqlContext = new SQLContext(sc)
        sqlContext.setConf("spark.io.compression.snappy.codec", "snappy")
        val frame: DataFrame = sqlContext.read.parquet("E:\\Test-workspace\\testSpark\\output\\project\\DMP\\parquet_less")
        frame.foreach(println)
        val ans: RDD[List[(String, Int)]] = frame.mapPartitions(k => {
            val re: Jedis = JedisPool.getMyrdis()
            k.map(row => {
                TagBusi.makeTags(row, re).toList
            })
        })
        ans.foreach(println)
    }
}
