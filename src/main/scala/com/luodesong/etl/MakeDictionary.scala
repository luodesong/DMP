package com.luodesong.etl

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 这个方法是为了构建appname应用名称的数据字典，方便于做广播变量
  * 洗出来的数据是如下形式：
  * (com.yj.daijia,1018代驾客户端)
  *     第一个字段也就是key的值是appname的id
  *     第二个字段也就是value的值是appname的name
  *
  * 做广播变量的目的是有的appname有的时候是空的就必须得通过id来在字典变量中查找出来
  *     inputPath：E:\Test-workspace\testSpark\input\project\DMP\dirc
  *     outputPath：E:\Test-workspace\testSpark\output\project\DMP\appAndIp
  */

object MakeDictionary {
    def main(args: Array[String]): Unit = {
        //判断路径
        if(args.length != 2) {
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
        val dictionaryLogs: RDD[String] = sc.textFile(inputPath)

        val tempRDD: RDD[Array[String]] = dictionaryLogs.map(x => {
            //"\\s"切割是的包括空格和\t的数据
            x.split("\t")
        }).filter(_.length >= 5)//如果数据小于了5的时候就是没用的垃圾数据
        val appAndIdRDD: RDD[(String, String)] = tempRDD.map(x => {
            (x(4),x(1))
        })

        //coalesce(1)将分区数设置为一个
        //saveAsTextFile(outputPath)保存出去
        appAndIdRDD.coalesce(1).saveAsTextFile(outputPath)
        sc.stop()

    }

}

