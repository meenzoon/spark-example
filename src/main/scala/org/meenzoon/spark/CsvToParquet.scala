package org.meenzoon.spark

import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.functions.{col}

object CsvToParquet {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Scala Spark SQL")
      .master("local")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()

    val path = "~/workspace/data"

    val df = spark.read.csv(path + "/file.csv").toDF
    val df2 = df.filter(col("_c8").gt(37.5))
    df2.write.parquet(path + "/output")
  }

}
