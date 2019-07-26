package com.daniel51.preprocess

import java.util

import ch.hsr.geohash.GeoHash
import com.alibaba.fastjson.{JSON, TypeReference}
import com.daniel51.common.util.SparkUtil
import com.hankcs.hanlp.HanLP
import com.hankcs.hanlp.seg.common.Term
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Dataset

import scala.collection.{JavaConverters, mutable}


/**
  * @date: 2019/7/23
  * @site: www.doitedu.cn
  * @author: hunter.d 涛哥
  * @qq: 657270652
  * @description: 流量日志预处理程序
  */
object EventLogPre {
  def main(args: Array[String]): Unit = {

    val spark = SparkUtil.getSparkSession("event_preprocess")
    import spark.implicits._

    // 读取原始日志
    // spark、mapreduce都能直接读取一些常见类型的压缩文件/gz/bz2/deflate/snappy....
    val ds = spark.read.textFile("data_warehouse/data/event_logs/2019-06-15")


    // 切割，提取json
    val json: Dataset[String] = ds.map(x => x.split(" --> ")(1))


    // 解析、过滤
    val filteredDs: Dataset[EventBean] = json.map(x => {

      var bean: EventBean = null;

      try {
        // 解析json
        val jSONObject = JSON.parseObject(x)
        val uobj = jSONObject.getJSONObject("u")
        val cookieid = uobj.getString("cookieid")
        val account = uobj.getString("account")

        val phoneObj = uobj.getJSONObject("phone")

        val imei = phoneObj.getString("imei")
        val osName = phoneObj.getString("osName")
        val osVer = phoneObj.getString("osVer")
        val resolution = phoneObj.getString("resolution")
        val androidId = phoneObj.getString("androidId")
        val manufacture = phoneObj.getString("manufacture")
        val deviceId = phoneObj.getString("deviceId")

        val appObj = uobj.getJSONObject("app")
        val appid = appObj.getString("appid")
        val appVer = appObj.getString("appVer")
        val release_ch = appObj.getString("release_ch")
        val promotion_ch = appObj.getString("promotion_ch")


        val locObj = uobj.getJSONObject("loc")

        val areacode = locObj.getString("areacode")
        val longtitude = locObj.getDouble("longtitude")
        val latitude = locObj.getDouble("latitude")
        val carrier = locObj.getString("carrier")
        val netType = locObj.getString("netType")

        val sessionId = uobj.getString("sessionId")

        val eventType = jSONObject.getString("logType")
        val commit_time = jSONObject.getLong("commit_time")

        val event = jSONObject.getJSONObject("event")

        import scala.collection.JavaConversions._
        val eventMap: util.Map[String, String] = event.getInnerMap.asInstanceOf[util.Map[String, String]]
        val eventMapScala = eventMap.toMap


        /*val keys = event.keySet()
        val eventMap: Map[String, String] = keys.map(key => (key, event.getString(key))).toMap*/

        bean = EventBean(
          cookieid,
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
          eventMapScala
        )

      } catch {
        case e: Exception => e.printStackTrace()
      }
      bean
    })
      .filter(bean => {
        // 这几个标识字段不能全为空
        val account = bean.account
        val androidId = bean.androidId
        val cookieid = bean.cookieid
        val deviceId = bean.deviceId
        val imei = bean.imei
        val sessionId = bean.sessionId

        val sb = new StringBuilder
        val pinjie = sb.append(account).append(androidId).append(cookieid).append(imei).append(sessionId).append(deviceId)
        val str = pinjie.replaceAllLiterally("null", "")

        StringUtils.isNotBlank(str)
      })


    // 读取地理位置字典、商圈字典
    val area = spark.read.parquet("data_warehouse/data/area_dict")

    // 将地理位置数据收集到driver端
    val areaMap: collection.Map[String, Array[String]] = area.map(row => {
      // "geo","province","city","district"
      val geo = row.getAs[String]("geo")
      val province = row.getAs[String]("province")
      val city = row.getAs[String]("city")
      val district = row.getAs[String]("district")
      (geo, Array(province, city, district))
    }).rdd.collectAsMap()

    val bc1 = spark.sparkContext.broadcast(areaMap)


    val biz = spark.read.parquet("data_warehouse/data/biz_dict")
    // 将商圈信息数据收集到driver端
    val bizMap: collection.Map[String, Array[String]] = biz.map(row => {
      // "geo","province","city","district","biz"
      val geo = row.getAs[String]("geo")
      val province = row.getAs[String]("province")
      val city = row.getAs[String]("city")
      val district = row.getAs[String]("district")
      val biz = row.getAs[String]("biz")
      (geo, Array(province, city, district, biz))
    }).rdd.collectAsMap()
    val bc2 = spark.sparkContext.broadcast(bizMap)

    // 集成地理位置信息
    val integrated: Dataset[EventBean] = filteredDs.map(bean => {

      val lng = bean.longtitude
      val lat = bean.latitude
      val geohash = GeoHash.withCharacterPrecision(lat, lng, 6).toBase32

      // 从广播变量中获取字典
      val areaDict = bc1.value
      val bizDict = bc2.value


      var province: String = ""
      var city: String = ""
      var district: String = ""
      var biz: String = ""


      var bizInfo: Array[String] = null
      bizInfo = bizDict.getOrElse(geohash, Array("", "", "", ""))
      province = bizInfo(0)
      city = bizInfo(1)
      district = bizInfo(2)
      biz = bizInfo(3)

      var areaInfo: Array[String] = null
      if (bizInfo(0).equals("")) {
        areaInfo = areaDict.getOrElse(geohash.substring(0, 5), Array("", "", ""))

        province = areaInfo(0)
        city = areaInfo(1)
        district = areaInfo(2)
      }

      bean.province = province
      bean.city = city
      bean.district = district
      bean.biz = biz

      bean
    })

    // 没有匹配上地理位置的那些gps坐标，需要另行输出
    integrated
      .filter(bean => StringUtils.isBlank(bean.province))
      .map(bean => (bean.longtitude, bean.latitude))
      .toDF("lng", "lat")
      .selectExpr("concat_ws(',',lng,lat)")
      .write
      .text("data_warehouse/data/gps_to_parse")


    // 加载停止词词典  TODO
    val stpwdSet = spark.read.textFile("data_warehouse/data/stopwords").rdd.collect().toSet
    val bc3 = spark.sparkContext.broadcast(stpwdSet)


    // 标题分词
    val result = integrated.map(bean => {
      val maybeTitle = bean.event.getOrElse("title", null)

      var keywords: String = ""
      if (maybeTitle != null) {

        val terms: util.List[Term] = HanLP.segment(maybeTitle)
        // TODO 过滤停止词的具体位置
        import scala.collection.JavaConversions._
        val stpwd = bc3.value
        keywords = terms.map(term => term.word).filter(word => {
          word.size > 1 && (!stpwd.contains(word))
        }).mkString(" ")

        val kwEventMap = bean.event.+("title_kwds" -> keywords)
        bean.event = kwEventMap
      }

      bean
    }).toDF()


    result
      .coalesce(1)
      .write
      .parquet("data_warehouse/data/out_eventlog_preprocess/2019-06-15")


    spark.close()

  }
}
