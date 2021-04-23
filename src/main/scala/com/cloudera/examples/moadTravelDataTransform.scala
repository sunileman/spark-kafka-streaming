package com.cloudera.examples

import org.apache.spark.sql.functions.{col, unix_timestamp}
import org.apache.spark.sql.{SaveMode, SparkSession}

object moadTravelDataTransform {

  def main(args: Array[String]): Unit = {

    val sourceFile = args(0)
    val targetLocation = args(1)


    println("\n*******************************")
    println("\n*******************************")
    println("\n**********INPUTS***************")
    println("\n**********INPUTS***************")
    println("\n**********INPUTS***************")
    println("Source File: " + sourceFile)
    println("Target File Location: " + targetLocation)
    println("\n*******************************")
    println("\n*******************************")


    val schema = covidschema.travelschema


    val spark = SparkSession.builder
      .appName("Transform Travel Data to Parquet")
      .getOrCreate()


    val df = spark.read
      .option("multiline","true")
      .json(sourceFile)

    df.printSchema()

    df.select(col("location"), col("data"), unix_timestamp().as("insert_date"))
      .write.mode(SaveMode.Append).parquet(targetLocation)

    /*
    df.select("location","data")
      .write.mode(SaveMode.Append).parquet(targetLocation)

     */

  }



}
