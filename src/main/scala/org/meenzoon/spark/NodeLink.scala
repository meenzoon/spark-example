package org.meenzoon.spark

import org.apache.sedona.core.formatMapper.shapefileParser.ShapefileReader
import org.apache.sedona.core.spatialRDD.SpatialRDD
import org.apache.sedona.spark.SedonaContext
import org.apache.sedona.sql.utils.Adapter
import org.apache.sedona.viz.core.Serde.SedonaVizKryoRegistrator
import org.apache.sedona.viz.sql.utils.SedonaVizRegistrator
import org.apache.spark.sql.functions.col
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

    rawSpatialDf.printSchema()

    val nodelinkDf = spark.sql(
      """
        | SELECT
        |   geometry, LINK_ID
        | FROM NODE_LINK_DF
        | WHERE ST_Distance(ST_Point(128.5320955, 35.8237748), geometry) <= 0.001
                                         """.stripMargin)

    nodelinkDf.show(false)
    println("spatialDF count: " + nodelinkDf.count())
    nodelinkDf.printSchema()

    nodelinkDf.write.format("geoparquet").save("output/nodelink.parquet")
  }
}
