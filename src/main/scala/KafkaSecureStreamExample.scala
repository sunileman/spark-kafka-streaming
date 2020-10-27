package com.cloudera.examples

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import scala.io.Source

object KafkaSecureStreamExample {

  def main(args: Array[String]): Unit = {

    val ksourcetopic = args(0)
    val ktargettopic = args(1)
    val kbrokers = args(2)

    println("\n*******************************")
    println("\n*******************************")
    println("source topic: "+ksourcetopic)
    println("target topic: "+ktargettopic)
    println("brokers: "+kbrokers)
    println("\n*******************************")
    println("\n*******************************")


    val spark = SparkSession.builder
      .appName("Spark Kafka Secure Structured Streaming Example")
      .config("spark.kafka.bootstrap.servers", kbrokers)
      .config("spark.kafka.sasl.kerberos.service.name", "kafka")
      .config("spark.kafka.security.protocol", "SASL_SSL")
      .config("spark.kafka.ssl.truststore.location", "/usr/lib/jvm/java-1.8.0/jre/lib/security/cacerts")
      //.config("spark.kafka.ssl.truststore.password", "changeit")
      .getOrCreate()

    import spark.implicits._

    spark.sparkContext.setLogLevel("INFO")



    val jsonStr = Source.fromURL("https://sunileman.s3.amazonaws.com/twitter/tweet1.json").mkString
    val twitterDataScheme = spark.read.json(Seq(jsonStr).toDS).toDF().schema


    val broadcastSchema = spark.sparkContext.broadcast(twitterDataScheme)



    //read twitter stream
    val df = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", kbrokers)
      .option("subscribe", ksourcetopic)
      .option("startingOffsets", "latest")
      .option("kafka.sasl.kerberos.service.name", "kafka")
      .option("kafka.ssl.truststore.location", "/usr/lib/jvm/java-1.8.0/jre/lib/security/cacerts")
      //.option("spark.kafka.ssl.truststore.password", "changeit")
      .option("kafka.security.protocol", "SASL_SSL")
      .option("failOnDataLoss", "false")
      .load()

    //extract only the text field from the tweet and write to a kafka topic
    val ds = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .filter($"value".contains("created_at"))
      .select(from_json($"value",schema = broadcastSchema.value).as("data")).select($"data".getItem("text").alias("value"))
      .writeStream.format("kafka")
      .outputMode("update")
      .option("kafka.bootstrap.servers", kbrokers)
      .option("topic", ktargettopic)
      .option("kafka.sasl.kerberos.service.name", "kafka")
      .option("kafka.ssl.truststore.location", "/usr/lib/jvm/java-1.8.0/jre/lib/security/cacerts")
      //.option("spark.kafka.ssl.truststore.password", "changeit")
      .option("kafka.security.protocol", "SASL_SSL")
      .option("checkpointLocation", "/app/mount/spark-checkpoint2")
      .start()
      .awaitTermination()


  }
}