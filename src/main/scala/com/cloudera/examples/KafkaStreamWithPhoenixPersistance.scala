package com.cloudera.examples
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.StringType

import scala.io.Source
import com.google.common.collect.ImmutableMap

object KafkaStreamWithPhoenixPersistance {

  def main(args: Array[String]): Unit = {

    val ksourcetopic = args(0)
    val kbrokers = args(1)
    val startingOffsets = args(2) //ie latest
    val checkpoint = args(3)  //ie s3 bucket
    val zkUrl= args(4)
    val lookupTblName= args(5)
    val targetTable = args(6)


    /*  val ksourcetopic = "moad-covid-json"
   val kbrokers = "moad-aw-dev0-dataflow-worker0.moad-aw.yovj-8g7d.cloudera.site:9093,moad-aw-dev0-dataflow-worker2.moad-aw.yovj-8g7d.cloudera.site:9093,moad-aw-dev0-dataflow-worker1.moad-aw.yovj-8g7d.cloudera.site:9093"
   val startingOffsets = "earliest"
   //val s3bucket = args(3)
   val checkpoint ="/tmp/spark-checkpoint1/"  //ie s3 bucket
   val zkUrl= "cod--or6q9la130qm-leader0.moad-aw.yovj-8g7d.cloudera.site,cod--or6q9la130qm-master0.moad-aw.yovj-8g7d.cloudera.site,cod--or6q9la130qm-master1.moad-aw.yovj-8g7d.cloudera.site:2181"
   val lookupTblName= "country_lookup"
   val ktargettopic = "moad-covid-json-merged" */

    println("\n*******************************")
    println("\n*******************************")
    println("\n**********INPUTS***************")
    println("\n**********INPUTS***************")
    println("\n**********INPUTS***************")
    println("source topic: "+ksourcetopic)
    println("brokers: "+kbrokers)
    println("startingOffsets: "+startingOffsets)
    // println("s3bucket: "+s3bucket)
    println("checkpoint: "+checkpoint)
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




    spark.sparkContext.setLogLevel("INFO")
    val phoenixConnector= new PhoenixConnector(spark,zkUrl,lookupTblName)
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




    val covidDataDf= dfreadstream.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
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
    val df_covidDataDf= covidDataDf.as("covid_data")


    // print(covidDataDf.count())
    val lookupDataDf= phoenixConnector.getLookupData(lookupTblName)


    //lookupDataDf.show()


    val finalDf=   covidDataDf.as("covid_data").join(lookupDataDf.as("lookup_data"),
      col("covid_data.country_code") === col("lookup_data.country_code"),"LEFTOUTER")
      .select(col("covid_data.location"),col("covid_data.country_code"),
        col("lookup_data.country_name"),
        col("covid_data.latitude"),col("covid_data.longitude"),col("covid_data.confirmed"),col("covid_data.dead"))

    lookupDataDf.unpersist();



    finalDf.writeStream.foreachBatch { (batchDF: DataFrame, batchId: Long) =>
    {
      batchDF.write.format("org.apache.phoenix.spark").mode(SaveMode.Overwrite)
        .options(ImmutableMap.of("driver","org.apache.phoenix.jdbc.PhoenixDriver","zkUrl",zkUrl,"table",targetTable))
        .save()
    }
    }.start().awaitTermination()





  }
}