package com.daniel51.dwd

import java.text.SimpleDateFormat
import java.util.Date

import com.daniel51.common.util.SparkUtil
import com.daniel51.preprocess.DWDTrafficBean
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{DataTypes, StructType}

/*
 @author Daniel51
 @DESCRIPTION DWD层流量数据分析明细表计算
 @create 2019/7/25
*/
object URLCT {

  /**
    * 工具方法，将一个给定的url根据字典找到所属类别
    *
    * @param urlDict
    * @param url
    * @return
    */
  //  def getUrlCT(urlDict: scala.collection.Map[String, String], url: String): String = {
  //    var ct = "其他"
  //    for (elem <- urlDict.keys) {
  //      if (url.startsWith(elem)) {
  //        ct = urlDict.getOrElse(elem, "其他")
  //      }
  //      ct
  //    }
  //}
  def getUrlCt(urlDict: scala.collection.Map[String, String], url: String): String = {
    var ct = "其他"
    for (prefix <- urlDict.keys) {
      if (url.startsWith(prefix)) ct = urlDict.getOrElse(prefix, "其他")
    }
    ct
  }


  /*
         * @author Daniel51
         * @DESCRIPTION: 对数据
        */
  def main(args: Array[String]): Unit = {
    val spark = SparkUtil.getSparkSession("dwd_traffic_dtl")

    import spark.implicits._
    // 读 ods_traffice_log,当日分区
    val ods_traffice_log = spark.read.parquet(args(0))

    // 读 url类别维度表  dim_url_ct
    val schema: StructType = new StructType()
    .add("prefix", DataTypes.StringType)
    .add("ct", DataTypes.StringType)
    /*    val dim_url_ct = spark.read.schema(schema).csv(args(1))

        val urlctMap = dim_url_ct.rdd.map(row => {
          val prefix = row.getAs[String]("prefix")
          val ct = row.getAs[String]("ct")
          (prefix, ct)
        }).collectAsMap()*/
    val dim_url_ct = spark.read.schema(schema).csv(args(1))
    // 将url类别维表收集到driver端
    val urlctMap = dim_url_ct.rdd.map(row => {
      val prefix = row.getAs[String]("prefix")
      val ct = row.getAs[String]("ct")
      (prefix, ct)
    }).collectAsMap()
    val bc: Broadcast[collection.Map[String, String]] = spark.sparkContext.broadcast(urlctMap)


    val result: DataFrame = ods_traffice_log
      .where("enventType='pg_view' and event['url'] is not null")
      .rdd.map(row => {
      val cookieid = row.getAs[String]("coocieid")
      val account = row.getAs[String]("account")
      val imei: String = row.getAs[String]("imei")
      val osName = row.getAs[String]("osName")
      val osVer = row.getAs[String]("osVer")
      val resolution = row.getAs[String]("resolution")
      val androidId = row.getAs[String]("androidId")
      val manufacture = row.getAs[String]("manufacture")
      val deviceId = row.getAs[String]("deviceId")
      val appid = row.getAs[String]("appid")
      val appVer = row.getAs[String]("appVer")
      val release_ch = row.getAs[String]("release_ch")
      val promotion_ch = row.getAs[String]("promotion_ch")
      val areacode = row.getAs[String]("areacode")
      val longtitude = row.getAs[Double]("longtitude")
      val latitude = row.getAs[Double]("latitude")
      val carrier = row.getAs[String]("carrier")
      val netType = row.getAs[String]("netType")
      val sessionId = row.getAs[String]("sessionId")
      val eventType = row.getAs[String]("eventType")
      val commit_time = row.getAs[Long]("commit_time")
      val event: collection.Map[String, String] = row.getMap[String, String](21)
      val province = row.getAs[String]("province")
      val city = row.getAs[String]("city")
      val district = row.getAs[String]("district")
      val biz = row.getAs[String]("biz")

      //      取出url
      val url = event.getOrElse("url", "")
      //      获取广播变量
      val urlDict = bc.value
      //      获取所属类别
      val ct: String = getUrlCt(urlDict, url)

      //      将时间戳转化为字符串
      val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
      val dateStr: String = dateFormat.format(new Date(commit_time))

      DWDTrafficBean(cookieid,
        account,
        imei,
        osName,
        osVer,
        resolution,
        androidId,
        manufacture,
        deviceId,
        appid,
        appVer,
        release_ch,
        promotion_ch,
        areacode,
        longtitude,
        latitude,
        carrier,
        netType,
        sessionId,
        eventType,
        commit_time,
        province,
        city,
        district,
        biz,
        ct,
        dateStr
      )

    }).toDF()
    result.write.parquet(args(0))
  }

}
