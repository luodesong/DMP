package com.questone

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

object Data2JsonAndMysqlBySparkCore {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf()
                .setAppName(this.getClass.getName)
                .setMaster("local[*]")

        val sc: SparkContext = new SparkContext(conf)

        val sqlContext: SQLContext = new SQLContext(sc)
        import sqlContext.implicits._

        //val Array(inputPath, outputPath) = args
        val logsRDD: RDD[String] = sc.textFile("E:\\Test-workspace\\testSpark\\input\\project\\DMP")

        val prAndCityRDD: RDD[(String, Int)] = logsRDD.map(_.split(",", -1)).filter(_.length >= 85).map(x => {
            (x(24) + "," + x(25), 1)
        })
        val proAndCityReduceRDD: RDD[(String, Int)] = prAndCityRDD.reduceByKey(_ + _)

        val ansRDD: RDD[ProAndCity] = proAndCityReduceRDD.map(x => {
            val strings: Array[String] = x._1.split(",")
            ProAndCity(x._2, strings(0), strings(1))
        })
        val ansDF: DataFrame = ansRDD.toDF()


        ansDF.write.partitionBy("provincename", "cityname").mode(SaveMode.Append).json("hdfs://min1:8020/sparktest")

        println(proAndCityReduceRDD.collect().toBuffer)
        sc.stop()
    }

}

case class ProAndCity(ct: Int, provincename: String, cityname: String)
