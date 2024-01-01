package org.meenzoon.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, udf}

object ItsSection {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("jj-its-section")
      .master("local")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()

    val path = "/Users/meenzoon/workspace/data"

    import spark.implicits._

    val df = spark.read
      .option("delimiter", "|")
      .option("header", "false")
      .csv(path + "/jj-its/new/*.txt")
      .filter($"_c0" > "2312261600000" && $"_c0" < "2312261630000")
      .sort(col("_c0").asc_nulls_first)
      .toDF()

    // 차량 번호별 group by
    val df2 = df.groupBy("_c6")
    df2.count().show(false)
    // UDF 함수 추가
    //val numAdd = udf`(NumUdf.numAdd _)`
    //df.withColumn("plus", numAdd(col("_c6"), col("_c7")))

    //df.printSchema()
    //df.show()

    spark.stop()
  }
}
