package com.luodesong.exam

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 1、按照pois，分类businessarea，并统计每个businessarea的总数。
  * 2、按照pois，分类type，为每一个Type类型打上标签，统计各标签的数量
  */
object TestExam {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
                .setAppName(this.getClass.getName)
                .setMaster("local[*]")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        val sc = new SparkContext(conf)

        val logsRDD: RDD[String] = sc.textFile("E:\\Test-workspace\\testSpark\\input\\testjson")
        //1、按照pois，分类businessarea，并统计每个businessarea的总数。
        val busiAndNamRDD: RDD[List[(String, (String, Int))]] = logsRDD.map(x => {
            val list: List[(String, (String, Int))] = JsonUtil.getList(x, 1).toList
            list
        })
        val busiAndNamGrupeFlatenRDD: RDD[(String, (String, Int))] = busiAndNamRDD.flatMap(x => x)
        val groued: RDD[(String, Iterable[(String, Int)])] = busiAndNamGrupeFlatenRDD.filter(!_._1.equals("[]")).groupByKey()
        val busAndCount: RDD[(String, Int)] = groued.map(x => {
            (x._1, x._2.size)
        }).sortBy(_._2,false)
        println("+++++++++++++++问题一+++++++++++++++++++++++")
        busAndCount.foreach(println)

        //2、按照pois，分类type，为每一个Type类型打上标签，统计各标签的数量
        val types: RDD[List[(String, (String, Int))]] = logsRDD.map(x => {
            val list: List[(String, (String, Int))] = JsonUtil.getList(x, 2).toList
            list
        })
        val groupedTow: RDD[(String, Iterable[(String, Int)])] = types.flatMap(x => x).groupByKey()
        val values: RDD[Iterable[(String, Int)]] = groupedTow.values
        val Types: RDD[(String, Int)] = values.map(x => {
            x.toList
        }).flatMap(x => x)
        val typeArr: RDD[Array[String]] = Types.map(x => {
            val strings: Array[String] = x._1.split(";")
            strings
        })
        val typeTup: RDD[(String, Int)] = typeArr.flatMap(x => x).map(x => (x, 1))
        val ans: RDD[(String, Int)] = typeTup.reduceByKey(_+_).sortBy(_._2,false)
        println("+++++++++++++++问题二+++++++++++++++++++++++")
        ans.foreach(println)
        sc.stop()

    }
}
