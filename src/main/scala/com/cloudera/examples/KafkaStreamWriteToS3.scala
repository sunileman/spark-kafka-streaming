package com.cloudera.examples

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.streaming.Trigger

object KafkaStreamWriteToS3 {

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
      .appName("Write COVID Cases to s3")
      .config("spark.kafka.bootstrap.servers", kbrokers)
      .config("spark.kafka.sasl.kerberos.service.name", "kafka")
      .config("spark.kafka.security.protocol", "SASL_SSL")
      .config("spark.kafka.sasl.mechanism", "PLAIN")
      .config("spark.kafka.ssl.truststore.location", "/usr/lib/jvm/java-1.8.0/jre/lib/security/cacerts")
      .config("spark.sql.streaming.checkpointLocation", checkpoint)
      .getOrCreate()

    spark.sparkContext.setLogLevel("INFO")

    import spark.implicits._

    //get schema and broadcast
    val jsonStr = covidschema.coviddata
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
        $"data".getItem("location").alias("location"),
        $"data".getItem("country_code").alias("country_code"),
        $"data".getItem("latitude").alias("latitude"),
        $"data".getItem("longitude").alias("longitude"),
        $"data".getItem("confirmed").alias("confirmed"),
        $"data".getItem("dead").alias("dead"),
        $"data".getItem("recovered").alias("recovered"),
        $"data".getItem("velocity_confirmed").alias("velocity_confirmed"),
        $"data".getItem("velocity_dead").alias("velocity_dead"),
        $"data".getItem("velocity_recovered").alias("velocity_recovered"),
        $"data".getItem("updated_date").alias("updated_date"),
        $"data".getItem("eventTimeLong").alias("eventTimeLong")
      )
      .writeStream
      .trigger(Trigger.ProcessingTime(20000))
      .outputMode("append")
      .format("parquet")
      .option("checkpointLocation", checkpoint)
      .start(s3bucket)
      .awaitTermination()


    /*
    //write stream to s3 as parquet
    dfreadstream.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .select(from_json($"value",schema = broadcastSchema.value).as("data")).select($"data".getItem("location").alias("location"))
      .writeStream
      //.withColumn("insert_timestamp",current_timestamp())
      .trigger(Trigger.ProcessingTime(20000))
      .format("parquet")
      .option("checkpointLocation", checkpoint)
      .start(s3bucket)
      .awaitTermination()



     */

  }
}
