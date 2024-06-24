package org.meenzoon.spark

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.SparkSession

object DtgToHbase {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("DtgToHBase")
      //.master("local")
      .master("yarn")
      .config("spark.scheduler.mode", "FAIR")
      .config("spark.driver.memory", "1G")
      .config("spark.executor.memory", "1G")
      .config("spark.executor.cores", "1")
      //.config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()

    //val path = "/Users/meenzoon/workspace/data"
    val path = "/data/data01"

    import spark.implicits._

    val df = spark.read
      .option("delimiter", "|")
      .option("header", "false")
      .csv(path + "/dtg/*.txt")

    df.printSchema()
    df.show()

    val df2 = df.rdd.mapPartitions(partition => {
      var putList: java.util.List[Put] = new java.util.ArrayList[Put];
      val conf = HBaseConfiguration.create()
      //conf.addResource(new Path(path + "/hbase-site.xml"))
      conf.addResource(new Path("/etc/hbase/conf/hbase-site.xml"))
      conf.set("hbase.zookeeper.quorum", "tims-nn01, tims-nn02, tims-dn08")
      conf.set("zookeeper.znode.parent", "/hbase-unsecure")
      conf.set("hbase.zookeeper.property.clientPort", "2181")
      val connection = ConnectionFactory.createConnection(conf)
      val table = connection.getTable(TableName.valueOf("dtg"))

      val newPartition = partition.map(record => {
        if(putList.size >= 10000) {
          table.put(putList)
          putList = new java.util.ArrayList[Put]
        }

        val put = new Put(Bytes.toBytes(record.get(1).toString + "|" + record.get(4).toString))
        put.addColumn(Bytes.toBytes("a"), Bytes.toBytes("vin"), Bytes.toBytes(record.get(0).toString))
        put.addColumn(Bytes.toBytes("a"), Bytes.toBytes("carRegNo"), Bytes.toBytes(record.get(1).toString))
        put.addColumn(Bytes.toBytes("a"), Bytes.toBytes("dailyDistance"), Bytes.toBytes(record.get(2).toString))
        put.addColumn(Bytes.toBytes("a"), Bytes.toBytes("totalDistance"), Bytes.toBytes(record.get(3).toString))
        put.addColumn(Bytes.toBytes("a"), Bytes.toBytes("datetime"), Bytes.toBytes(record.get(4).toString))
        put.addColumn(Bytes.toBytes("a"), Bytes.toBytes("speed"), Bytes.toBytes(record.get(5).toString))
        put.addColumn(Bytes.toBytes("a"), Bytes.toBytes("rpm"), Bytes.toBytes(record.get(6).toString))
        put.addColumn(Bytes.toBytes("a"), Bytes.toBytes("brakeSignal"), Bytes.toBytes(record.get(7).toString))
        put.addColumn(Bytes.toBytes("a"), Bytes.toBytes("longitude"), Bytes.toBytes(record.get(8).toString))
        put.addColumn(Bytes.toBytes("a"), Bytes.toBytes("latitude"), Bytes.toBytes(record.get(9).toString))
        put.addColumn(Bytes.toBytes("a"), Bytes.toBytes("azimuth"), Bytes.toBytes(record.get(10).toString))
        put.addColumn(Bytes.toBytes("a"), Bytes.toBytes("accX"), Bytes.toBytes(record.get(11).toString))
        put.addColumn(Bytes.toBytes("a"), Bytes.toBytes("accY"), Bytes.toBytes(record.get(12).toString))
        put.addColumn(Bytes.toBytes("a"), Bytes.toBytes("status"), Bytes.toBytes(record.get(13).toString))
        putList.add(put)
      }).toList

      //val connection = ConnectionFactory.createConnection(conf)
      //val table = connection.getTable(TableName.valueOf("dtg"))
      table.put(putList)
      table.close()
      connection.close()
      newPartition.iterator
    })
    df2.count

    spark.stop()
  }
}
