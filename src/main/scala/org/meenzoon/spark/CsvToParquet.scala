package org.meenzoon.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, udf}
import org.meenzoon.spark.utils.NumUdf

object CsvToParquet {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Scala Spark SQL")
      .master("local")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()

    import spark.implicits._
    val path = "/Users/meenzoon/workspace/data"

    val df = spark.read
      .option("delimeter", ",").option("header", "true")
      .csv(path + "/file.csv")
      .filter(col("_c8").gt(37.5))

    // UDF 함수 추가
    val numAdd = udf(NumUdf.numAdd _)
    df.withColumn("plus", numAdd(col("_c6"), col("_c7")))

    df.show()
  }

}
