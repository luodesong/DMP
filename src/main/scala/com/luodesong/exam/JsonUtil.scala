package com.luodesong.exam

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import scala.collection.mutable.ListBuffer

object JsonUtil {
    def getList(str: String, flag: Int): ListBuffer[(String, (String, Int))] = {
        val json: JSONObject = JSON.parseObject(str)
        val status: Int = json.getIntValue("status")
        status match {
            case v if (v == 0) => null
            case _ => {
                val regeocodeJson: JSONObject = json.getJSONObject("regeocode")
                regeocodeJson match {
                    case v if (v == null || regeocodeJson.keySet().isEmpty) => null
                    case _ => {
                        val pois: JSONArray = regeocodeJson.getJSONArray("pois")
                        pois match {
                            case v if (v == null || pois.isEmpty) => null
                            case _ => {
                                val buffer: ListBuffer[(String, (String, Int))] = ListBuffer[(String, (String, Int))]()
                                for (k <- pois.toArray()) {
                                    val js: JSONObject = k.asInstanceOf[JSONObject]
                                    if (k.isInstanceOf[JSONObject]) {
                                        if (flag == 1) {
                                            val businessarea: String = js.getString("businessarea")
                                            val busiessNema: String = js.getString("name")
                                            buffer.append((businessarea, (busiessNema, 1)))
                                        } else {
                                            val types: String = js.getString("type")
                                            buffer.append(("types", (types, 1)))
                                        }

                                    }
                                }
                                buffer
                            }
                        }
                    }
                }
            }
        }
    }
}
