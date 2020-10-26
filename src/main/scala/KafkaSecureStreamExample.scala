package com.cloudera.examples

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import scala.io.Source

object KafkaSecureStreamExample {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Spark Kafka Secure Structured Streaming Example")
      .getOrCreate()

    import spark.implicits._


    val ksourcetopic = args(0)
    val kbrokers = args(1)

    println("\n*******************************")
    println("\n*******************************")
    println("source topic: "+ksourcetopic)
    println("brokers: "+kbrokers)
    println("\n*******************************")
    println("\n*******************************")



    //read twitter stream
    val df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", kbrokers)
      .option("kafka.sasl.kerberos.service.name", "kafka")
      .option("kafka.ssl.truststore.location", "/usr/lib/jvm/java-1.8.0/jre/lib/security/cacerts")
      .option("spark.kafka.ssl.truststore.password", "changeit")
      .option("kafka.security.protocol", "SASL_SSL")
      .option("subscribe", ksourcetopic)
      .load()
      .selectExpr("CAST(value AS STRING)")

    val words = df.as[String].flatMap(_.split(" "))
    val wordCounts = words.groupBy("value").count()


    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()




  }
}