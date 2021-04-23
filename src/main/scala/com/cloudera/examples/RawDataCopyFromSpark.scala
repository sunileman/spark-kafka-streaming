package com.cloudera.examples

object RawDataCopyFromSpark {

  import org.apache.hadoop.fs.FileSystem
  import org.apache.spark.sql.SparkSession

  /*
  val access_key_amazon = "accessKey.amazon"
  val secret_key_amazon = "secretKey.amazon"
  val session: SparkSession = SparkSession.builder.master("local").config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem").config("spark.hadoop.fs.s3a.access.key", access_key_amazon).config("spark.hadoop.fs.s3a.secret.key", secret_key_amazon).config("fs.s3a.connection.ssl.enabled", "false").config("spark.network.timeout", "600s").config("spark.executor.heartbeatInterval", "500s").getOrCreate
  session.sparkContext.hadoopConfiguration.set("mapreduce.fileoutputcommitter.algorithm.version", "2")
  val srcPath = new Nothing("Source_File_Location")
  val srcFs: FileSystem = FileSystem.get(srcPath.toUri, session.sparkContext.hadoopConfiguration)
  val dstPath = new Nothing("Target_Location")
  val dstFs: FileSystem = FileSystem.get(dstPath.toUri, session.sparkContext.hadoopConfiguration)
  //FileUtil.copy(srcFs, srcPath, dstFs, dstPath, false, false, session.sparkContext.hadoopConfiguration)
  */

}
