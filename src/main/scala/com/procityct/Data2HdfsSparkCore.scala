package com.procityct

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * inputPath: E:\Test-workspace\testSpark\input\project\DMP
  * outputPath: hdsf://min1:8020/sparktest/procity
  */
object Data2HdfsSparkCore {
    def main(args: Array[String]): Unit = {
        //判断路径
        if (args.length != 2) {
            println("目录参数不正确，退出程序")
            sys.exit()
        }

        // 创建一个集合保存输入输出产数
        val Array(inputPath, outputPath) = args
        val conf: SparkConf = new SparkConf()
                .setAppName(this.getClass.getName)
                .setMaster("local[*]")

        val sc: SparkContext = new SparkContext(conf)

        val sqlContext: SQLContext = new SQLContext(sc)
        import sqlContext.implicits._

        //val Array(inputPath, outputPath) = args
        val logsRDD: RDD[String] = sc.textFile(inputPath)

        val prAndCityRDD: RDD[(String, Int)] = logsRDD.map(_.split(",", -1)).filter(_.length >= 85).map(x => {
            (x(24) + "," + x(25), 1)
        })
        val proAndCityReduceRDD: RDD[(String, Int)] = prAndCityRDD.reduceByKey(_ + _)

        val ansRDD: RDD[ProAndCity] = proAndCityReduceRDD.map(x => {
            val strings: Array[String] = x._1.split(",")
            ProAndCity(x._2, strings(0), strings(1))
        })
        val ansDF: DataFrame = ansRDD.toDF()


        ansDF.write.partitionBy("provincename", "cityname").mode(SaveMode.Append).json(outputPath )

        println(proAndCityReduceRDD.collect().toBuffer)
        sc.stop()
    }

}

case class ProAndCity(ct: Int, provincename: String, cityname: String)
