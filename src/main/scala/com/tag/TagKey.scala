package com.tag

import com.tag.thetrait.Tag
import org.apache.spark.sql.Row

import scala.collection.mutable.ListBuffer

object TagKey extends Tag {
    /**
      * 统一标签的方法
      */
    override def makeTags(args: Any*): ListBuffer[(String, Int)] = {
        val myList: ListBuffer[(String, Int)] = ListBuffer[(String, Int)]()

        val row: Row = args(0).asInstanceOf[Row]

        val keywords: String = row.getAs[String]("keywords")
        val strings: Array[String] = keywords.split("\\|")
        for (k <- strings) {
            if (k.length >= 3 && k.length <= 8)
            myList.append(("K" + k, 1))
        }
        myList
    }
}
