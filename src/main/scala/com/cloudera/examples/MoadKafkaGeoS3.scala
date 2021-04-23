package com.cloudera.examples

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.streaming.Trigger

object MoadKafkaGeoS3 {

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
      .appName("Spark Kafka Secure Structured Streaming Example")
      .config("spark.kafka.bootstrap.servers", kbrokers)
      .config("spark.kafka.sasl.kerberos.service.name", "kafka")
      .config("spark.kafka.security.protocol", "SASL_SSL")
      .config("spark.kafka.sasl.mechanism", "PLAIN")
      .config("spark.kafka.ssl.truststore.location", "/usr/lib/jvm/java-1.8.0/jre/lib/security/cacerts")
      .config("spark.sql.streaming.checkpointLocation", "/app/mount/spark-checkpoint5")
      .getOrCreate()

    try {
      import spark.implicits._


      //get schema and broadcast
      val jsonStr = covidschema.geoapidata
      val geoDataScheme = spark.read.json(Seq(jsonStr).toDS).toDF().schema
      geoDataScheme.printTreeString()
      val broadcastSchema = spark.sparkContext.broadcast(geoDataScheme)

      //read covid stream

      val df = spark.readStream.format("kafka")
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


      df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
        .select(from_json($"value", schema = broadcastSchema.value).as("data"))
        .select(
          $"data".getItem("city").alias("city"),
          $"data".getItem("countryCode").alias("countryCode"),
          $"data".getItem("postalCode").alias("postalCode"),
          $"data".getItem("countryName").alias("countryName"),
          $"data".getItem("latitude").alias("latitude"),
          $"data".getItem("longitude").alias("longitude")
        )
        .writeStream
        .trigger(Trigger.ProcessingTime(20000))
        .outputMode("append")
        .format("parquet")
        .option("checkpointLocation", checkpoint)
        .start(s3bucket)
        .awaitTermination()



    } finally {
      spark.sparkContext.stop()
    }


  }
}
