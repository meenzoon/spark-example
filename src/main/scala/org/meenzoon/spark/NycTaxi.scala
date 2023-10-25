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
    df.printSchema()

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
