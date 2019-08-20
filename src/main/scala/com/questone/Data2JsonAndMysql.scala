package com.questone

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 统计各省市数据量分布情况
  */
object Data2JsonAndMysql {
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
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

        //创建执行入口
        val sc: SparkContext = new SparkContext(conf)
        val sqlContext: SQLContext = new SQLContext(sc)

        //设置压缩方式
        sqlContext.setConf("spark.io.compression.snappy.codec", "snappy")

        //读取parquet文件
        val logsDF: DataFrame = sqlContext.read.parquet(inputPath)

        //创建临时文件
        logsDF.registerTempTable("logs")

        val sqlString: String = "select count(*) ct, provincename,cityname from logs group by provincename,cityname sort by ct desc"
        val ansDF: DataFrame = sqlContext.sql(sqlString)
        ansDF.show()
        //ansDF.write.mode(SaveMode.Append).json(outputPath)
        val pro = new Properties()
        pro.put("user", "root")
        pro.put("password", "123456")
        val url = "jdbc:mysql://localhost:3306/spark_test"
        ansDF.write.mode(SaveMode.Append).jdbc(url, "t1", pro)

        sc.stop()
    }

}
