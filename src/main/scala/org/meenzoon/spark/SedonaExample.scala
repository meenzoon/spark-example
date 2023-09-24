package org.meenzoon.spark

import org.apache.sedona.core.formatMapper.shapefileParser.ShapefileReader
import org.apache.sedona.core.spatialRDD.SpatialRDD
import org.apache.sedona.core.utils.SedonaConf
import org.apache.sedona.sql.utils.{Adapter, SedonaSQLRegistrator}
import org.apache.sedona.viz.core.Serde.SedonaVizKryoRegistrator
import org.apache.sedona.viz.sql.utils.SedonaVizRegistrator
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.locationtech.jts.geom.{Coordinate, Geometry, GeometryFactory}

object SedonaExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Scala Spark SQL")
      .master("local")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[SedonaVizKryoRegistrator].getName)
      .getOrCreate()

    SedonaSQLRegistrator.registerAll(spark)
    SedonaVizRegistrator.registerAll(spark)

    //val resourceFolder = System.getProperty("user.dir") + "/src/test/resources/"
    val resourceFolder = "src/test/resources/"

    val csvPolygonInputLocation = resourceFolder + "testenvelope.csv"
    val csvPointInputLocation = resourceFolder + "testpoint.csv"
    val shapefileInputLocation = resourceFolder + "shapefiles/dbf"
    val rasterdatalocation = resourceFolder + "raster/"

    var spatialRDD = new SpatialRDD[Geometry]
    spatialRDD = ShapefileReader.readToGeometryRDD(spark.sparkContext, shapefileInputLocation)
    var rawSpatialDf = Adapter.toDf(spatialRDD, spark)
    rawSpatialDf.createOrReplaceTempView("rawSpatialDf")
    var spatialDf = spark.sql(
      """
        | SELECT geometry, STATEFP, COUNTYFP
        | FROM rawSpatialDf
                                         """.stripMargin)
    spatialDf.show()
    spatialDf.printSchema()

  }
}

