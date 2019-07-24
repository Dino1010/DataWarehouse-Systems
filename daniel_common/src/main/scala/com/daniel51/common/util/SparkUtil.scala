package com.daniel51.common.util

import org.apache.spark.sql.SparkSession

/*
 @author Daniel51
 @DESCRIPTION ${DESCRIPTION}
 @create 2019/7/24
*/
object SparkUtil {
  def getSparkSession(appName:String,master:String="local[*]")={

    SparkSession.builder().appName(appName).master(master).getOrCreate()

  }
}
