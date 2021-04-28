package com.cloudera.examples

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.streaming.Trigger

object BingCovidApi2s3 {

  def main(args: Array[String]): Unit = {

    val ksourcetopic = args(0)
    val kbrokers = args(1)
    val startingOffsets = args(2) //ie latest
    val s3bucket = args(3)
    val checkpoint = args(4) //ie s3 bucket


    println("\n*******************************")
    println("\n*******************************")
    println("\n**********INPUTS***************")
    println("\n**********INPUTS***************")
    println("\n**********INPUTS***************")
    println("source topic: " + ksourcetopic)
    println("brokers: " + kbrokers)
    println("startingOffsets: " + startingOffsets)
    println("s3bucket: " + s3bucket)
    println("checkpoint: " + checkpoint)
    println("\n*******************************")
    println("\n*******************************")


    val spark = SparkSession.builder
      .appName("Write Bing COVID Stream Data to s3")
      .config("spark.kafka.bootstrap.servers", kbrokers)
      .config("spark.kafka.sasl.kerberos.service.name", "kafka")
      .config("spark.kafka.security.protocol", "SASL_SSL")
      .config("spark.kafka.sasl.mechanism", "PLAIN")
      .config("spark.kafka.ssl.truststore.location", "/usr/lib/jvm/java-1.8.0/jre/lib/security/cacerts")
      .config("spark.sql.streaming.checkpointLocation", "/app/mount/spark-checkpoint5")
      .getOrCreate()

    spark.sparkContext.setLogLevel("INFO")

    import spark.implicits._

    //get schema and broadcast
    val jsonStr = covidschema.bingcovidapi
    val covidDataScheme = spark.read.json(Seq(jsonStr).toDS).toDF().schema
    covidDataScheme.printTreeString()
    val broadcastSchema = spark.sparkContext.broadcast(covidDataScheme)

    //read moad stream
    val dfreadstream = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", kbrokers)
      .option("subscribe", ksourcetopic)
      .option("startingOffsets", startingOffsets)
      .option("kafka.sasl.kerberos.service.name", "kafka")
      .option("kafka.ssl.truststore.location", "/usr/lib/jvm/java-1.8.0/jre/lib/security/cacerts")
      .option("kafka.security.protocol", "SASL_SSL")
      .option("kafka.sasl.mechanism", "PLAIN")
      .option("failOnDataLoss", "false")
      .option("checkpointLocation", checkpoint)
      .load()


    dfreadstream.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .select(from_json($"value", schema = broadcastSchema.value).as("data"))
      .select(
        $"data".getItem("ID").alias("ID"),
        $"data".getItem("Updated").alias("Updated"),
        $"data".getItem("Confirmed").alias("Confirmed"),
        $"data".getItem("ConfirmedChange").alias("ConfirmedChange"),
        $"data".getItem("Deaths").alias("Deaths"),
        $"data".getItem("DeathsChange").alias("DeathsChange"),
        $"data".getItem("Recovered").alias("Recovered"),
        $"data".getItem("RecoveredChange").alias("RecoveredChange"),
        $"data".getItem("Latitude").alias("Latitude"),
        $"data".getItem("Longitude").alias("Longitude"),
        $"data".getItem("ISO2").alias("ISO2"),
        $"data".getItem("ISO3").alias("ISO3"),
        $"data".getItem("Country_Region").alias("Country_Region"),
        $"data".getItem("AdminRegion1").alias("AdminRegion1"),
        $"data".getItem("AdminRegion2").alias("AdminRegion2")
      )
      .writeStream
      .trigger(Trigger.ProcessingTime(20000))
      .outputMode("append")
      .format("parquet")
      .option("checkpointLocation", checkpoint)
      .start(s3bucket)
      .awaitTermination()


  }
}
