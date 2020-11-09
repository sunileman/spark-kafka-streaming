import org.apache.avro.generic.GenericData.StringType
import org.apache.spark.sql._
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

object KafkaSecureStreamSimpleLocalExample {

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
      .master("local")
      .config("spark.kafka.bootstrap.servers", kbrokers)
      .config("spark.kafka.sasl.kerberos.service.name", "kafka")
      .config("spark.kafka.security.protocol", "SASL_SSL")
      .config("kafka.sasl.mechanism", "PLAIN")
      .config("spark.driver.extraJavaOptions", "-Djava.security.auth.login.config=/Users/sunile.manjee/Documents/Cloudera/javacode/spark-kafka-streaming/src/main/resources/jaas.conf")
      .config("spark.executor.extraJavaOptions", "-Djava.security.auth.login.config=/Users/sunile.manjee/Documents/Cloudera/javacode/spark-kafka-streaming/src/main/resources/jaas.conf")
      .config("spark.kafka.ssl.truststore.location", "/Users/sunile.manjee/Documents/Cloudera/javacode/spark-kafka-streaming/truststore.jks")
      .getOrCreate()

    spark.sparkContext.setLogLevel("INFO")


    val mySchema = StructType(Array(
      StructField("id", org.apache.spark.sql.types.StringType),
      StructField("text", org.apache.spark.sql.types.StringType)
    ))

    val streamingDataFrame = spark.readStream.schema(mySchema).format("csv").load("/Users/sunile.manjee/Documents/Cloudera/javacode/spark-kafka-streaming/src/main/resources/data")

    val ds = streamingDataFrame.selectExpr("CAST(id AS STRING)", "CAST(text AS STRING) as value")
      .writeStream.format("kafka")
      .outputMode("update")
      .option("kafka.bootstrap.servers", kbrokers)
      .option("topic", ktargettopic)
      .option("kafka.sasl.kerberos.service.name", "kafka")
      .option("kafka.ssl.truststore.location", "/Users/sunile.manjee/Documents/Cloudera/javacode/spark-kafka-streaming/truststore.jks")
      .option("kafka.security.protocol", "SASL_SSL")
      .option("kafka.sasl.mechanism", "PLAIN")
      //.option("checkpointLocation", "s3a://yours3bucket/datalake/sunman/spark-checkpoint2")
      .option("checkpointLocation", "/tmp/spark-checkpoint2/")
      .start()
      .awaitTermination()

  }
}