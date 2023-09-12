package org.meenzoon.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class CsvToParquet {
  public static void main(String[] args) {
    SparkSession spark = SparkSession
        .builder()
        .appName("Java Spark SQL")
        .getOrCreate();

    Dataset<Row> df = spark.read().csv("path/input/file.csv").toDF();

    df.write().parquet("path/output/file.parquet");
  }
}