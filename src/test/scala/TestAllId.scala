import com.luodesong.tag.{TagAdv, TagAppRedis, TagBusi, TagCha, TagEqu, TagKey, TagLoc}
import com.luodesong.util.{JedisPool, TagUtil}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.junit.Test
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

class TestAllId {
    @Test
    def testOne(): Unit = {
        /*
        val conf = new SparkConf()
                .setAppName(this.getClass.getName).setMaster("local[*]")
                // 处理数据，采取scala的序列化方式，性能比Java默认的高
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        val sc = new SparkContext(conf)

        // 我们要采取snappy压缩方式， 因为咱们现在用的是1.6版本的spark，到2.0以后呢，就是默认的了
        // 可以不需要配置
        val sqlContext = new SQLContext(sc)
        sqlContext.setConf("spark.io.compression.snappy.codec", "snappy")

        //读取的是过滤文件：用于过滤的是关键字，比如禁用词语
        val deric1RDD: RDD[String] = sc.textFile("E:\\Test-workspace\\testSpark\\input\\project\\DMP\\keyFilter")
        //广播变量，关键字过滤
        val broadcast1: Broadcast[Array[String]] = sc.broadcast(deric1RDD.collect())

        //读取点击流日志
        val df: DataFrame = sqlContext.read.parquet("E:\\Test-workspace\\testSpark\\output\\project\\DMP\\parquet_less")
        val useridsAndRowRDD: RDD[(List[String], Row)] = df.filter(TagUtil.oneUserId).map(row => {
            val userIds: List[String] = TagUtil.getAnyAllUserId(row)
            (userIds, row)

        })
        useridsAndRowRDD.map(x => {
            val idAndRowRDD: List[(String, Row)] = x._1.map(t => {
                (t, x._2)
            })

        })



        //处理组合结果list
        /**
          * 先按照key分组聚合
          */
        val userIdAndTags: RDD[(String, List[(String, Int)])] = userIdAndTag.reduceByKey((list1, list2) => (list1 ::: list2))
        val ans: RDD[(String, List[(String, Int)])] = userIdAndTags.map(x => {
            /**
              * 在每个key的value值中在进行一次分组求和
              */
            val stringToInt: List[(String, Int)] = x._2.groupBy(_._1).mapValues(x => x.size).toList
            (x._1, stringToInt)
        })

        sc.stop()

         */
    }

}
