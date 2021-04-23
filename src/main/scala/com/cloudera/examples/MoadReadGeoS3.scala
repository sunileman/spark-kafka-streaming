package com.cloudera.examples


import org.apache.spark.sql.SparkSession
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials

import scala.collection.JavaConversions._
import com.amazonaws.services.s3.iterable.S3Objects
import com.amazonaws.services.s3.model.CopyObjectRequest
import com.typesafe.config.ConfigFactory

import scala.sys.exit


object MoadReadGeoS3 {

  def main(args: Array[String]): Unit = {


    val sources3bucket = args(0)
    val sourcefolder = args(1)
    val targets3bucket = args(2)
    val targetfolder = args(3)
    val deleteSourceFile = args(4)


    println("creating spark session")
    val spark = SparkSession.builder
      .appName("MOAD read ReverseGeoJson")
      .getOrCreate()

    spark.sparkContext.setLogLevel("INFO")

    val s3Client = AmazonS3ClientBuilder.standard().build();
    //val credentials = new BasicAWSCredentials(defaultConfig.getString("awscreds.AWS_ACCESS_KEY"), defaultConfig.getString("awscreds.AWS_SECRET_KEY"))
    //val s3Client = AmazonS3ClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(credentials)).build()



    println("\n*******************************")
    println("\n*******************************")
    println("\n**********INPUTS***************")
    println("\n**********INPUTS***************")
    println("\n**********INPUTS***************")
    println("sources3bucket: " + sources3bucket)
    println("sourcefolder: " + sourcefolder)
    println("targets3bucket: " + targets3bucket)
    println("targetfolder: " + targetfolder)
    println("\n*******************************")
    println("\n*******************************")


    try {
      println("Moving files from source to target folders")
      for (summary <- S3Objects.withPrefix(s3Client, sources3bucket, sourcefolder)) {
        val objkey = summary.getKey
        println("Found: s3://" + sources3bucket + "/" + objkey)
        val filename = objkey.substring(objkey.lastIndexOf("/") + 1)
        if (objkey.contains(".json")) {
          /*
          works but i'm going to use copy request
          println("moving: s3://" + sources3bucket + "/" + objkey+ " to s3://" + targetfolder + "/" + targetfolder + "/filename")
          val x = s3Client.copyObject(sources3bucket, objkey, targets3bucket, targetfolder + "/" + filename)
          print(x)
          println("moved: "+summary.getKey)
           */
          println("moving: s3://" + sources3bucket + "/" + objkey+ " to s3://" + targetfolder + "/" + targetfolder + "/filename")
          val creq = new CopyObjectRequest(sources3bucket, objkey, targets3bucket, targetfolder + "/" + filename)
          val cres = s3Client.copyObject(creq)
          print(cres)

          if (deleteSourceFile.toBoolean && !cres.getVersionId.isEmpty) {
            println("Deleting: "+summary.getKey)
            s3Client.deleteObject(sources3bucket, objkey)
          }
        }
        else {
          println("Not a json, deleting: "+ sources3bucket+"/"+objkey)
          s3Client.deleteObject(sources3bucket, objkey)
        }

      }
    }finally {
      println("Finished Moving Files")
      exit()
    }

    /*
    println("creating spark session")
    val spark = SparkSession.builder
      .appName("MOAD read ReverseGeoJson")
      .getOrCreate()

    try {
      import spark.implicits._


      //get schema and broadcast
      val jsonStr = covidschema.covidreversegeo
      val geoDataScheme = spark.read.json(Seq(jsonStr).toDS).toDF().schema
      geoDataScheme.printTreeString()
      val broadcastSchema = spark.sparkContext.broadcast(geoDataScheme)

      //read covid stream

      val dfgeojson = spark.read.schema(geoDataScheme).json(targets3bucket+targetfolder)

      dfgeojson.write.orc(targets3bucket+targetfolder)

      //need to move from processed to done


    } finally {
      spark.sparkContext.stop()
    }

     */


  }


}


