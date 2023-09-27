package org.meenzoon.spark

import org.apache.spark.sql.SparkSession

object NycTaxi {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession
      .builder()
      .appName("Scala Spark SQL")
      .master("local")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()

    val df = sparkSession.read.parquet("/Users/meenzoon/workspace/data/nyc-taxi/*.parquet")

    df.show()
    df.printSchema()

    sparkSession.stop()
  }
}
