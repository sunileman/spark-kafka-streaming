package com.cloudera.examples

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.streaming.Trigger

object KafkaStreamWriteToS3Local {

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
      val jsonStr = covidschema.coviddata
      val covidDataScheme = spark.read.json(Seq(jsonStr).toDS).toDF().schema
      covidDataScheme.printTreeString()
      val broadcastSchema = spark.sparkContext.broadcast(covidDataScheme)

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

      /*
      df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
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
        .outputMode("append")
        .format("console")
        .start()
        .awaitTermination()


       */
      df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
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
        .option("checkpointLocation", "/tmp/checkpoint")
        .start("/Users/sunile.manjee/Documents/DeleteMe/cde")
        .awaitTermination()



      /*
      //write covid stream to console
      df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
        .select(from_json($"value", schema = broadcastSchema.value).as("data")).select($"data".getItem("location").alias("location"))
        .writeStream
        .outputMode("append")
        .format("console")
        .start()
        .awaitTermination()


       */


      /*this works
      val covidds = spark.createDataset("""{"location":"Pilõezinhos","country_code":"br","latitude":-6.843131,"longitude":-35.53058,"confirmed":87,"dead":0,"recovered":"null","velocity_confirmed":5,"velocity_dead":0,"velocity_recovered":0,"updated_date":"2020-06-13 00:20:20.628942+00:00","eventTimeLong":1592007620000}""":: Nil)
      val covid2 = spark.read.json(covidds)
      covid2.show()
      covid2.select("location").show()

       */


    } finally {
      spark.sparkContext.stop()
    }


  }
}
