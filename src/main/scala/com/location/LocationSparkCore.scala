package com.location

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object LocationSparkCore {
    def main(args: Array[String]): Unit = {
        // 模拟企业开发模式，首先判断一下目录 是否为空
        if(args.length != 2){
            println("目录不正确，退出程序！")
            sys.exit()
        }
        // 创建一个集合，存储一下输入输出目录
        val Array(inputPath,outputPath) = args
        val conf = new SparkConf()
                .setAppName(this.getClass.getName).setMaster("local")
                // 处理数据，采取scala的序列化方式，性能比Java默认的高
                .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
        val sc = new SparkContext(conf)
        // 我们要采取snappy压缩方式， 因为咱们现在用的是1.6版本的spark，到2.0以后呢，就是默认的了
        // 可以不需要配置
        val sqlContext = new SQLContext(sc)
        sqlContext.setConf("spark.io.compression.snappy.codec","snappy")
        val df = sqlContext.read.parquet(inputPath)
        // 通过调用算子的方式处理数据
        df.map(row=> {
            // 先去获取需要的参数， 原始 有效 广告...
            val requestmode = row.getAs[Int]("requestmode")
            val processnode = row.getAs[Int]("processnode")
            val iseffective = row.getAs[Int]("iseffective")
            val isbilling = row.getAs[Int]("isbilling")
            val isbid = row.getAs[Int]("isbid")
            val iswin = row.getAs[Int]("iswin")
            val adorderid = row.getAs[Int]("adorderid")
            val winprice = row.getAs[Double]("winprice")
            val adpayment = row.getAs[Double]("adpayment")
        })
    }

}
