package com.cloudera.examples

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.from_json

object readlocaljson {

  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder
      .appName("Simple CDE Run Example")
      .master("local")
      .getOrCreate()

    val df = spark.read.json("file:///Users/sunile.manjee/Documents/Cloudera/javacode/spark-kafka-streaming/src/main/resources/covidjh.json")

    df.printSchema()

    import spark.implicits._
    val jsonStr = covidschema.covidjhapi
    val covidDataScheme = spark.read.json(Seq(jsonStr).toDS).toDF().schema
    covidDataScheme.printTreeString()
    val broadcastSchema = spark.sparkContext.broadcast(covidDataScheme)
    //df.show(5)
    df.select("Country","Province","Date","Type","Count","Difference","Source", "Country Latest", "Latitude", "Longitude")
      .na.drop(Seq("Country"))
      .withColumnRenamed("Date","epoch_date")
      .withColumnRenamed("Count","type_count")
      .withColumnRenamed("Country Latest","Country_Latest")
      .show(5)
    /*
   df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .select(from_json($"value", schema = broadcastSchema.value).as("data"))
      .select(
        $"data".getItem("Province").alias("Province")).show(5)

     */
  }



}
