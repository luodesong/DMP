package com.tag

import com.tag.thetrait.Tag
import org.apache.spark.sql.Row

import scala.collection.mutable.ListBuffer

object TagLoc extends Tag {
    /**
      * 统一标签的方法
      */
    override def makeTags(args: Any*): ListBuffer[(String, Int)] = {
        val myList: ListBuffer[(String, Int)] = ListBuffer[(String, Int)]()

        val row: Row = args(0).asInstanceOf[Row]

        val rtbprovince: String = row.getAs[String]("provincename")
        val rtbcity: String = row.getAs[String]("cityname")

        myList.append(("ZP" + rtbprovince, 1))
        myList.append(("ZC" + rtbcity, 1))
        myList
    }
}
