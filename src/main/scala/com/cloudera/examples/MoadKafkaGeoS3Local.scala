package com.cloudera.examples

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.from_json

object MoadKafkaGeoS3Local {

  def main(args: Array[String]): Unit = {

    val ksourcetopic = args(0)
    val kbrokers = args(1)


    println("\n*******************************")
    println("\n*******************************")
    println("\n**********INPUTS***************")
    println("\n**********INPUTS***************")
    println("\n**********INPUTS***************")
    println("source topic: " + ksourcetopic)
    println("brokers: " + kbrokers)
    println("\n*******************************")
    println("\n*******************************")


    val spark = SparkSession.builder
      .appName("Spark Kafka Secure Structured Streaming Example")
      .master("local")
      .config("spark.kafka.bootstrap.servers", kbrokers)
      .config("spark.kafka.sasl.kerberos.service.name", "kafka")
      .config("spark.kafka.security.protocol", "SASL_SSL")
      .config("kafka.sasl.mechanism", "PLAIN")
      .config("spark.driver.extraJavaOptions", "-Djava.security.auth.login.config=./src/main/resources/jaas.conf")
      .config("spark.executor.extraJavaOptions", "-Djava.security.auth.login.config=./src/main/resources/jaas.conf")
      .config("spark.kafka.ssl.truststore.location", "./src/main/resources/truststore.jks")
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
        .option("startingOffsets", "earliest")
        .option("kafka.sasl.kerberos.service.name", "kafka")
        .option("kafka.ssl.truststore.location", "./src/main/resources/truststore.jks")
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "PLAIN")
        .option("checkpointLocation", "/tmp/spark-checkpoint2/")
        .option("failOnDataLoss", "false")
        .load()

      df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
        .select(from_json($"value", schema = broadcastSchema.value).as("data"))
        .select(
          $"data".getItem("city").alias("city"),
          $"data".getItem("countryCode").alias("countryCode"),
          $"data".getItem("postalCode").alias("postalCode"),
          $"data".getItem("countryName").alias("countryName"),
          $"data".getItem("lat").alias("latitude"),
          $"data".getItem("long").alias("longitude")
        )
        .writeStream
        .outputMode("append")
        .format("console")
        .start()
        .awaitTermination()


    } finally {
      spark.sparkContext.stop()
    }


  }
}
