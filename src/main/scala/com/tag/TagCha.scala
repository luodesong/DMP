package com.tag

import com.tag.thetrait.Tag
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

import scala.collection.mutable.ListBuffer

/**
  * 渠道的标签
  */
object TagCha extends Tag{
    /**
      * 统一标签的方法
      */
    override def makeTags(args: Any*): ListBuffer[(String, Int)] = {
        val myList: ListBuffer[(String, Int)] = ListBuffer[(String, Int)]()

        val row: Row = args(0).asInstanceOf[Row]

        val adplatformproviderid: Int = row.getAs[Int]("adplatformproviderid")
        myList.append(("CN" + adplatformproviderid, 1))

        myList
    }
}
