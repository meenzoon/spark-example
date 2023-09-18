package org.meenzoon.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;

public class CsvToParquetJ {
  public static void main(String[] args) {
    SparkSession spark = SparkSession
        .builder()
        .appName("Java Spark SQL")
        .master("local")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate();

    String path = "~/workspace/data/";

    Dataset<Row> df = spark.read().csv(path + "/file.csv").toDF();
    df.filter(col("_c8").gt(37.5)).write().parquet(path + "/file.parquet");

    spark.stop();
  }
}
