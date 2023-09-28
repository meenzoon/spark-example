package org.meenzoon.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.sum

object NycTaxi {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession
      .builder()
      .appName("Scala Spark SQL")
      .master("local")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()
    import sparkSession.implicits._

    val df = sparkSession.read.parquet("/Users/meenzoon/workspace/data/nyc-taxi/*.parquet")

    df.select(sum($"total_amount")).show()

    df.show()
    df.printSchema()

    sparkSession.stop()
  }
}
