package org.meenzoon.spark

import org.apache.sedona.core.formatMapper.shapefileParser.ShapefileReader
import org.apache.sedona.core.spatialRDD.SpatialRDD
import org.apache.sedona.spark.SedonaContext
import org.apache.sedona.sql.utils.Adapter
import org.apache.sedona.viz.core.Serde.SedonaVizKryoRegistrator
import org.apache.sedona.viz.sql.utils.SedonaVizRegistrator
import org.locationtech.jts.geom.Geometry

object NodeLink {
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

    val shapefileInputLocation = resourceFolder + "shapefiles/nodelink"
    var spatialRDD = new SpatialRDD[Geometry]
    spatialRDD = ShapefileReader.readToGeometryRDD(spark.sparkContext, shapefileInputLocation)
    val rawSpatialDf = Adapter.toDf(spatialRDD, spark)
    rawSpatialDf.createOrReplaceTempView("NODE_LINK_DF")
    val nodelinkDf = spark.sql(
      """
        | SELECT *
        | FROM NODE_LINK_DF
        | WHERE ST_Distance(ST_POINT(128.5283423, 35.8272649), geometry) > 0.01
                                         """.stripMargin)
    nodelinkDf.show()
    println("spatialDF count: " + nodelinkDf.count())
    nodelinkDf.printSchema()
  }
}

