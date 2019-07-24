import com.daniel51.common.util.SparkUtil

object ParuqetReader {

  def main(args: Array[String]): Unit = {


    val spark = SparkUtil.getSparkSession("test")
    val res = spark.read.parquet("data_warehouse/data/area_dict")

    res.show(10,false)

    println(res.count())


    spark.close()
  }

}
