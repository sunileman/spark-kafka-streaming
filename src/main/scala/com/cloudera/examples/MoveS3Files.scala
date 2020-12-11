package com.cloudera.examples
/*
SUPER IMPORTANT
For this to work you need to add the assume role to the liftie instances created by CDE
also make sure the lifeie instances have s3 access to the buckets you are reading/writing
 */
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.iterable.S3Objects
import com.amazonaws.services.s3.model.CopyObjectRequest
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConversions._
import scala.sys.exit


object MoveS3Files {

  def main(args: Array[String]): Unit = {


    //ie mysocurcebucket
    val sources3bucket = args(0)
    //ie myfolder/asubfolder
    val sourcefolder = args(1)
    //ie look only for .json so the input would be .json
    val sourceFileFilter = args(2)
    //ie mytargetbucket
    val targets3bucket = args(3)
    //ie myfolder/anothersubfolder
    val targetfolder = args(4)
    // ie false
    val deleteSourceFile = args(5)


    println("creating spark session")
    val spark = SparkSession.builder
      .appName("Move s3 files")
      .getOrCreate()

    spark.sparkContext.setLogLevel("INFO")


    val s3Client = AmazonS3ClientBuilder.standard().build();


    println("\n*******************************")
    println("\n*******************************")
    println("\n**********INPUTS***************")
    println("\n**********INPUTS***************")
    println("\n**********INPUTS***************")
    println("sources3bucket: " + sources3bucket)
    println("sourcefolder: " + sourcefolder)
    println("sourcefolder: " + sourceFileFilter)
    println("targets3bucket: " + targets3bucket)
    println("targetfolder: " + targetfolder)
    println("deleteSourceFile: " + deleteSourceFile)
    println("\n*******************************")
    println("\n*******************************")




    try {
      println("Moving files from source to target folders")
      for (summary <- S3Objects.withPrefix(s3Client, sources3bucket, sourcefolder)) {
        val objkey = summary.getKey
        println("Found: s3://" + sources3bucket + "/" + objkey)
        val filename = objkey.substring(objkey.lastIndexOf("/") + 1)
        println("Checking if "+objkey.toLowerCase+" contains "+sourceFileFilter.toLowerCase)
        if (objkey.toLowerCase.contains(sourceFileFilter.toLowerCase)) {
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


  }


}


