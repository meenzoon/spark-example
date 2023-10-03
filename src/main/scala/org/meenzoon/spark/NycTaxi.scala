package org.meenzoon.spark

import org.apache.sedona.viz.core.Serde.SedonaVizKryoRegistrator
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession

object NycTaxi {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession
      .builder()
      .appName("Scala Spark SQL")
      .master("local")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[SedonaVizKryoRegistrator].getName)
      .getOrCreate()
    import sparkSession.implicits._

    val df = sparkSession.read.parquet("/Users/meenzoon/workspace/data/nyc-taxi/*.parquet")

    df.show()

    //df.select(sum($"total_amount")).show()

    implicit val mapEncoder = org.apache.spark.sql.Encoders.kryo[Map[String, Any]]

    val df2 = df.rdd.map(line => {
      val field1 = line(3)
      val field2 = line(4)

      val transformedField1 = field1.## + 1
      val transformedField2 = field2.##
      // 변환된 필드들을 다시 콤마로 연결하여 반환
      (line, 1)
      //(line, s"$transformedField1,$transformedField2")
    }).toDS()

    df2.show()
    df2.printSchema()

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

    sparkSession.stop()
  }
}
