package org.meenzoon.spark

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{max, sum}

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

    //df.select(sum($"total_amount")).show()

    val df2 = df.rdd.map(line => {
      val field1 = line(3)
      val field2 = line(4)

      val transformedField1 = field1.toString
      val transformedField2 = field2.toString
      // 변환된 필드들을 다시 콤마로 연결하여 반환
      s"$transformedField1,$transformedField2"
    })

    df2.foreach(println)

    //df2.toDS().show()
    //df2.toDS().printSchema()

    //val df3 = df2.reduce((x, y) => x + y)
    /*
    df2.toDS().write.mode(SaveMode.Overwrite)
      .option("compression", "gzip")
      .csv("/Users/meenzoon/workspace/data/output/")
    */

    //val df3 = df2.reduce((x, y) => x + y)
      //.reduceByKey((x, y) => x + y).toDS()

    //df3.show()
    //df3.printSchema()

    df.show()
    df.printSchema()

    sparkSession.stop()
  }
}
