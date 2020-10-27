import sys
from pyspark.sql import SparkSession
if __name__ == "__main__":

    brokerAddress = "kafka-broker1.se-sandb.a465-9q4k.cloudera.site:9093"

    spark = SparkSession.builder.\
    config('spark.kafka.bootstrap.servers', brokerAddress).\
    config('spark.kafka.sasl.kerberos.service.name', 'kafka').\
    config('spark.kafka.security.protocol', 'SASL_SSL').\
    config('spark.kafka.ssl.truststore.location', '/usr/lib/jvm/java-1.8.0/jre/lib/security/cacerts').\
    config('spark.kafka.ssl.truststore.password', 'changeit').\
    getOrCreate()

    spark.sparkContext.setLogLevel("INFO")

    from pyspark.sql.types import *
    from pyspark.sql.functions import *

    kafkaDF = spark.readStream.format("kafka").option("kafka.bootstrap.servers", brokerAddress).option("startingOffsets", "latest").option("subscribe", "wordsin").option("kafka.security.protocol", "SASL_SSL").option("kafka.sasl.kerberos.service.name", "kafka").option("kafka.ssl.truststore.location", "/usr/lib/jvm/java-1.8.0/jre/lib/security/cacerts").option("kafka.ssl.truststore.password", "changeit").option("failOnDataLoss", "false").load()

    # stringValueDF = kafkaDF.select(kafkaDF.value.cast("string")).alias("csv").select("csv.*")

    stringValueDF = kafkaDF.select(kafkaDF.value.cast("string"))

    # Filter the transactions that has is_fraud column set to 1
    # parsedDF = stringValueDF.withColumn("cc_num", split("csv.value", "\\|").getItem(0)).withColumn("is_fraud", split("csv.value", "\\|").getItem(10)).filter("is_fraud = 1")


    # Fraud transactions are written to cc_frauds kafka topic
    strQuery = stringValueDF.writeStream.outputMode("update").format("kafka").option("kafka.bootstrap.servers", brokerAddress).option("topic", "wordsout").option("kafka.security.protocol", "SASL_SSL").option("kafka.sasl.kerberos.service.name", "kafka").option("kafka.ssl.truststore.location", '/usr/lib/jvm/java-1.8.0/jre/lib/security/cacerts').option("checkpointLocation", "/app/mount/spark-checkpoint").start()

    strQuery.awaitTermination()



