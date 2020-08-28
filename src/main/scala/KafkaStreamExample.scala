package com.cloudera.examples

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming._
import sys.process._
import scala.io.Source
import java.io.File
import java.io.PrintWriter

import org.apache.spark.sql.functions._

import org.apache.spark.sql._

object KafkaStreamExample {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Spark Kafka Structured Streaming Example")
      .getOrCreate()

    import spark.implicits._


    val ksourcetopic = args(0)
    val ktargettopic = args(1)
    val kborkers = args(2)

    println("\n*******************************")
    println("\n*******************************")
    println("source topic: "+ksourcetopic)
    println("target topic: "+ktargettopic)
    println("brokers: "+kborkers)
    println("\n*******************************")
    println("\n*******************************")


    val jsonStr = Source.fromURL("https://sunileman.s3.amazonaws.com/twitter/tweet1.json").mkString
    val twitterDataScheme = spark.read.json(Seq(jsonStr).toDS).toDF().schema



    val broadcastSchema = spark.sparkContext.broadcast(twitterDataScheme)


    //read twitter stream
    val df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", kborkers).option("subscribe", ksourcetopic).load()

    //extract only the text field from the tweet and write to a kafka topic
    val ds = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .filter($"value".contains("created_at"))
      .select(from_json($"value",schema = broadcastSchema.value).as("data")).select($"data".getItem("text").alias("value"))
      .writeStream.format("kafka")
      .option("kafka.bootstrap.servers", kborkers)
      .option("topic", ktargettopic).option("checkpointLocation", "/tmp/s/checkpoint28")
      .start().awaitTermination()

  }
}