package com.cloudera.examples

import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{SaveMode, SparkSession}

object moadBingTransform {

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


    val schema = covidschema.bingschema


    val spark = SparkSession.builder
      .appName("Transform Bing Covid Data to Parquet")
      .getOrCreate()

    val df = spark.read.format("csv")
      .option("header","true")
      .schema(schema)
      .load(sourceFile)


    df.printSchema()


    df.select("ID","Updated","Confirmed","ConfirmedChange","Deaths","DeathsChange","Recovered", "RecoveredChange", "Latitude", "Longitude", "ISO2", "ISO3", "Country_Region", "AdminRegion1", "AdminRegion2" )
      .write.mode(SaveMode.Append).parquet(targetLocation)

  }



}
