package com.cloudera.examples

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame
class PhoenixConnector(spark:SparkSession,zkUrl:String,tableName:String) extends Serializable{

  val sqlContext= new SQLContext(spark.sparkContext)

  def getLookupData(table:String): DataFrame  ={
    val df =sqlContext
      .read
      .format("org.apache.phoenix.spark")
      .option("table",table)
      .option("zkUrl",zkUrl)
      .load()

    df



  }

}