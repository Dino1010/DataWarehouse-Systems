package com.daniel51.dicts

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

/*
 @author Daniel51
 @DESCRIPTION ${DESCRIPTION}
 @create 2019/7/22
*/
object test {
  Logger.getLogger("org").setLevel(Level.WARN)
  def main(args: Array[String]): Unit = {
    val ssc=  SparkSession.builder().appName(this.getClass.getSimpleName).master("local").getOrCreate()
    import ssc.implicits._
    val dataFrame: DataFrame = ssc.createDataset(Seq(
      "a",
      "a",
      "b",
      "b",
      "a",
      "c")).toDF("word")
    dataFrame.createTempView("wc")

    ssc.sql(
      """
        |
        |select
        |word,count(1)
        |from
        |wc
        |group by word
        |
      """.stripMargin).show(10,false)
//
    dataFrame.rdd.map((_,1)).reduceByKey(_+_).take(10).foreach(println)

    ssc.close()
  }

}
