package com.luodesong.graphx

import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object GraphxTest {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
                .setAppName(this.getClass.getName).setMaster("local[*]")
                // 处理数据，采取scala的序列化方式，性能比Java默认的高
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        val sc = new SparkContext(conf)

        // 我们要采取snappy压缩方式， 因为咱们现在用的是1.6版本的spark，到2.0以后呢，就是默认的了
        // 可以不需要配置
        val sqlContext = new SQLContext(sc)
        sqlContext.setConf("spark.io.compression.snappy.codec", "snappy")

        val pointRDD: RDD[(Long, (String, Int))] = sc.makeRDD(Seq(
            (1L, ("詹姆斯", 35)),
            (2L, ("霍华德", 34)),
            (6L, ("杜兰特", 31)),
            (9L, ("库里", 30)),
            (133L, ("哈登", 30)),
            (138L, ("席尔瓦", 36)),
            (16L, ("法尔考", 35)),
            (44L, ("内马尔", 27)),
            (21L, ("J罗", 28)),
            (5L, ("高斯林", 60)),
            (7L, ("奥德斯基", 55)),
            (158L, ("马云", 55))
        ))

        //构建边
        val egdeRDD: RDD[Edge[Int]] = sc.makeRDD(Seq(
            Edge(1L, 133L, 0),
            Edge(2L, 133L, 0),
            Edge(6L, 133L, 0),
            Edge(9L, 133L, 0),
            Edge(6L, 138L, 0),
            Edge(16L, 138L, 0),
            Edge(44L, 138L, 0),
            Edge(21L, 138L, 0),
            Edge(5L, 158L, 0),
            Edge(7L, 158L, 0)
        ))

        //构图
        val graph: Graph[(String, Int), Int] = Graph(pointRDD, egdeRDD)

        //取出每个边的最大顶点
        val vertices: VertexRDD[VertexId] = graph.connectedComponents().vertices
        //vertices.foreach(println)

        /**
          * RDD[(VertexId01, (VertexId02, (String, Int)))] :
          *     VertexId01：原来的点的id
          *     VertexId02：是最大点的id
          *     (String, Int)：原始的数据
          */
        val vertexIdAndMaxpoinAndvalue: RDD[(VertexId, (VertexId, (String, Int)))] = vertices.join(pointRDD)
        //temp.foreach(println)


        val MaxpoinAndValue: RDD[(VertexId, List[(String, Int)])] = vertexIdAndMaxpoinAndvalue.map {
            case (userId1, (userId2, (name, age))) => {
                (userId2, List((name, age)))
            }
        }
        //聚合起来
        val ans: RDD[(VertexId, List[(String, Int)])] = MaxpoinAndValue.reduceByKey(_++_)
        ans.foreach(println)
        sc.stop()
    }

}
