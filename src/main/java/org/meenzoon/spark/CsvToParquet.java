package org.meenzoon.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;

public class CsvToParquet {
  public static void main(String[] args) {
    SparkSession spark = SparkSession
        .builder()
        .appName("Java Spark SQL")
        .master("local")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate();

    Dataset<Row> df = spark.read().csv("file.csv").toDF();
    df.filter(col("g").gt(37.5)).write().parquet("file.parquet");

    spark.stop();
  }
}
