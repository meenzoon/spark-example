package org.meenzoon.spark

import org.apache.sedona.core.formatMapper.shapefileParser.ShapefileReader
import org.apache.sedona.core.spatialRDD.SpatialRDD
import org.apache.sedona.spark.SedonaContext
import org.apache.sedona.sql.utils.Adapter
import org.apache.sedona.viz.core.Serde.SedonaVizKryoRegistrator
import org.apache.sedona.viz.sql.utils.SedonaVizRegistrator
import org.apache.spark.serializer.KryoSerializer
import org.locationtech.jts.geom.Geometry

object SedonaExample {
  def main(args: Array[String]): Unit = {
    val config = SedonaContext.builder()
      .appName("Scala Spark SQL")
      .master("local")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.kryo.registrator", classOf[SedonaVizKryoRegistrator].getName)
      .getOrCreate()
    val spark = SedonaContext.create(config)
    SedonaVizRegistrator.registerAll(spark)

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
    println("spatialDF count: " + spatialDf.count())
    spatialDf.printSchema()

    spatialDf.write
      .mode("overwrite")
      .format("geoparquet")
      .save("/Users/meenzoon/workspace/data/geoparquet")
  }
}

