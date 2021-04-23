package com.cloudera.examples

import org.apache.spark.sql.SparkSession

object helloworld_with_args {

  def main(args: Array[String]): Unit = {


    val myarg1 = args(0)

    println("\n*******************************")
    println("\n*******************************")
    println("\n**********THIS IS A TEST***************")
    println("myarg1: "+myarg1)
    println("\n*******************************")
    println("\n*******************************")


    val spark = SparkSession.builder
      .appName("Simple Spark Test with args")
      .getOrCreate()



  }
}
