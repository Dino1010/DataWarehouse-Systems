package com.daniel51.dicts

import java.util.Properties

import ch.hsr.geohash.GeoHash
import com.daniel51.common.util.SparkUtil
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._

/*
 @author Daniel51
 @DESCRIPTION ${DESCRIPTION}
 @create 2019/7/24
*/
object AreaDicts2 {

  Logger.getLogger("areaDict").setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {
    val ssc: SparkSession = SparkUtil.getSparkSession(this.getClass.getSimpleName)
    import ssc.implicits._

//    加载数据文件
    val ds= ssc.read.textFile("data_warehouse/data/area_orgin/省市区GPS坐标数据.sql")
//      .where("lng is not null and lat is not null")


    val area = ds.map(line => {
      val arr: Array[String] = line.split(", ")
      val id = arr(0).split("\\(")(1)
      val name = arr(1).substring(1, arr(1).length - 1)
      val parentId = arr(2)
      val level = arr(4)
      val lat = arr(arr.length - 1)
      val lng = arr(arr.length - 2).split("\\)")(0)
      (id,name,parentId,lat,lat,lng)
    })
    area.show(10,false)

//    area.rdd.foreach(println)


    //    关闭连接
    ssc.close()


  }

}
