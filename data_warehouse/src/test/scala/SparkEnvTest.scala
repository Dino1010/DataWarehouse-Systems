import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object SparkEnvTest {

  Logger.getLogger("org").setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder().appName("test").master("local").getOrCreate()
    import spark.implicits._

    val ds: DataFrame = spark.createDataset(Seq(
      "a",
      "a",
      "b",
      "b",
      "a",
      "c")).toDF("word")

    // DSL 风格
    ds.groupBy("word").count().show(10,false)


    ds.createTempView("wc")
    spark.sql(
      """
        |
        |select
        |word,count(1)
        |from wc
        |group by word
        |
        |
      """.stripMargin)
        .show(10,false)



    ds.rdd.map((_,1)).reduceByKey(_ + _).take(10).foreach(println)

    println("hello gitee")

    spark.close()



  }

}
