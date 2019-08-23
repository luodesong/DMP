package com.luodesong.procityct

import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}
/**
  * 统计各省市数据量分布情况
  *     inputPath：E:\Test-workspace\testSpark\output\project\DMP\parquet
  *     outputPath：mysql
  */
object Data2Mysql {
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

        //创建临时表
        logsDF.registerTempTable("logs")
        val sqlString: String = "select count(*) ct, provincename,cityname from logs group by provincename,cityname sort by ct desc"
        val ansDF: DataFrame = sqlContext.sql(sqlString)
        ansDF.show()

        //加载数据库连接的配置文件
        val load: Config = ConfigFactory.load("application.properties")
        val prop: Properties = new Properties()
        prop.setProperty("user", load.getString("jdbc.user"))
        prop.setProperty("password", load.getString("jdbc.password"))

        //将数据写出到mysql
        ansDF.write.mode(SaveMode.Append).jdbc(load.getString("jdbc.url"), load.getString("jdbc.table"), prop)
        sc.stop()
    }

}
