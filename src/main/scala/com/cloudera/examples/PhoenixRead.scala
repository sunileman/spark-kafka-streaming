package com.cloudera.examples

import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}

object PhoenixRead {


  def main(args: Array[String]): Unit = {


    val spark = SparkSession
      .builder()
      .appName("phoenix-test")
      .master("local")
      .getOrCreate()
    try {


      // Load data from TABLE1
      val hbase_zookeeper_url = args(0)
      val lookupTblName= args(1)
      val df = spark.sqlContext
        .read
        .format("org.apache.phoenix.spark")
        .options(Map("table" -> lookupTblName, "zkUrl" -> hbase_zookeeper_url))
        .load

      df.show()
      df.count()

    } finally {
      spark.stop()


    }

  }

}