package com.daniel51.dicts

import java.util.Properties

import ch.hsr.geohash.GeoHash
import com.daniel51.common.util.SparkUtil
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

/*
 @author Daniel51
 @DESCRIPTION ${DESCRIPTION}
 @create 2019/7/24
*/
object AreaDicts {
  Logger.getLogger("areaDict").setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {
    val ssc: SparkSession = SparkUtil.getSparkSession(this.getClass.getSimpleName)
    import ssc.implicits._
    val url = "jdbc:mysql://localhost:3306/dicts"
    val props = new Properties()
    props.setProperty("user", "root")
    props.setProperty("password", "123456")
    val dataFrame: DataFrame = ssc.read.jdbc(url, "area_dict", props)
      .where("lng is not null and lat is not null")

    //老师原装
    val res = dataFrame.rdd.map({
      case Row(lng: Double, lat: Double,province: String, city: String, district: String ) => {
        var geoHash: String = null;
        //TODO 为什么不生效
//        if (lng != null && lat != null) {
          geoHash = GeoHash.withCharacterPrecision(lat, lng, 5).toBase32
//        }
        (geoHash, province, city, district)
      }
    }).toDF("geo", "province", "city", "district")

    res.write.mode(SaveMode.Overwrite).parquet("data_warehouse/data/area_dict")


//    res.show(10, false)



    //df=>rdd=>newdf
/*    val res = dataFrame.rdd.map(row=>{
      row.
    })
    dataFrame.printSchema()*/


    //    关闭连接
    ssc.close()


  }

}
