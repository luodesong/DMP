package com.tag

import com.tag.thetrait.Tag
import org.apache.spark.sql.Row

import scala.collection.mutable.ListBuffer

/**
  * appName的标签
  */
object TagApp extends Tag{
    /**
      * 统一标签的方法
      */
    override def makeTags(args: Any*): ListBuffer[(String, Int)] = {
        val myList: ListBuffer[(String, Int)] = ListBuffer[(String, Int)]()

        val row: Row = args(0).asInstanceOf[Row]

        //将广播变量给拿进来
        val directory: Map[String,String] = args(1).asInstanceOf[Map[String,String]]

        var appname: String = row.getAs[String]("appname")
        val appid: String = row.getAs[String]("appid")

        if (appname.equals("") || appname.equals(" ")) {
            appname = directory.getOrElse(appid, "0")
        }
        myList.append(("APP " + appname, 1))
        myList
    }
}
