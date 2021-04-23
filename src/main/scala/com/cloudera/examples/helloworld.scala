package com.cloudera.examples

import org.apache.spark.sql.SparkSession

object helloworld {

  def main(args: Array[String]): Unit = {


    println("\n*******************************")
    println("\n*******************************")
    println("\n**********THIS IS A TEST***************")

    println("\n*******************************")
    println("\n*******************************")


    val spark = SparkSession.builder
      .appName("Simple Spark Test")
      .getOrCreate()



  }
}
