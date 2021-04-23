package com.cloudera.examples
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql._
import com.google.common.collect.ImmutableMap
object moadEmpGeo {
  def main(args: Array[String]): Unit = {
    val sourceFile = args(0)
    val targetPhoenixTable = args(1) //ie latest
    val zkUrl = args(2) //ie latest
    val checkpoint = args(3) //ie s3 bucket
    println("\n*******************************")
    println("\n*******************************")
    println("\n**********INPUTS***************")
    println("\n**********INPUTS***************")
    println("\n**********INPUTS***************")
    println("Source File: " + sourceFile)
    println("targetPhoenixTable: " + targetPhoenixTable)
    println("zkUrl: " + zkUrl)
    println("checkpoint: " + checkpoint)
    println("\n*******************************")
    println("\n*******************************")
    val spark = SparkSession.builder
      .appName("Fetch employee geo data")
      .getOrCreate()
    //val df = spark.read.json("file:///Users/sunile.manjee/Documents/Cloudera/javacode/spark-kafka-streaming/src/main/resources/covidjh.json")
    val df = spark.read.parquet(sourceFile)
    df.printSchema()

    df.write.format("org.apache.phoenix.spark").mode(SaveMode.Overwrite)
      .options(ImmutableMap.of("driver","org.apache.phoenix.jdbc.PhoenixDriver","zkUrl",zkUrl,"table",targetPhoenixTable))
      .save()

  }


}