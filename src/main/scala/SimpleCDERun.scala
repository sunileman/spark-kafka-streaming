package com.cloudera.examples

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import scala.io.Source

object SimpleCDERun {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Simple CDE Run Example")
      .getOrCreate()

    import spark.implicits._


    val imputmessage = args(0)

    println("\n*******************************")
    println("\n*******************************")
    println("source s3 file: "+imputmessage)
    println("\n*******************************")
    println("\n*******************************")



  }
}