package com.cloudera.examples

import org.apache.spark.sql.SparkSession

object KafkaSecureStreamSimpleExample {

  def main(args: Array[String]): Unit = {

    val ksourcetopic = args(0)
    val ktargettopic = args(1)
    val kbrokers = args(2)

    //for example s3a://goes-se-cdp-sandbox/datalake/data/sunman/spark-checkpoint2
    val checkpointLocation = args(3)

    println("\n*******************************")
    println("\n*******************************")
    println("source topic: " + ksourcetopic)
    println("target topic: " + ktargettopic)
    println("brokers: " + kbrokers)
    println("\n*******************************")
    println("\n*******************************")


    val spark = SparkSession.builder
      .appName("Spark Kafka Secure Structured Streaming Example")
      .config("spark.kafka.bootstrap.servers", kbrokers)
      .config("spark.kafka.sasl.kerberos.service.name", "kafka")
      .config("spark.kafka.security.protocol", "SASL_SSL")
      .config("spark.kafka.ssl.truststore.location", "/usr/lib/jvm/java-1.8.0/jre/lib/security/cacerts")
      .getOrCreate()

    spark.sparkContext.setLogLevel("INFO")


    //read from the input kafka topic
    val df = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", kbrokers)
      .option("subscribe", ksourcetopic)
      .option("startingOffsets", "latest")
      .option("kafka.sasl.kerberos.service.name", "kafka")
      .option("kafka.ssl.truststore.location", "/usr/lib/jvm/java-1.8.0/jre/lib/security/cacerts")
      .option("kafka.security.protocol", "SASL_SSL")
      .option("failOnDataLoss", "false")
      .load()


    //pull the message from the input kafka topic and write back to output topic
    val ds = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .writeStream.format("kafka")
      .outputMode("update")
      .option("kafka.bootstrap.servers", kbrokers)
      .option("topic", ktargettopic)
      .option("kafka.sasl.kerberos.service.name", "kafka")
      .option("kafka.ssl.truststore.location", "/usr/lib/jvm/java-1.8.0/jre/lib/security/cacerts")
      .option("kafka.security.protocol", "SASL_SSL")
      .option("checkpointLocation", checkpointLocation)
      .start()
      .awaitTermination()


  }
}
