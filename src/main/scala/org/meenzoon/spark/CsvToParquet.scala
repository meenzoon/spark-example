package org.meenzoon.spark

import org.apache.spark.sql.{SparkSession}

object CsvToParquet {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Scala Spark SQL")
      .getOrCreate()

    val df = spark.read.csv("path/input/file.csv")

    df.write.parquet("path/output/file.parquet")
  }

}