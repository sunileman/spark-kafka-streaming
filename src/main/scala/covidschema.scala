package com.cloudera.examples
import org.apache.spark.sql.types.{BooleanType, DataTypes, DoubleType, IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.SparkConf
import org.apache.spark.sql.Column
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.Row.empty.schema
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions
import org.apache.spark.sql.streaming.StreamingQuery
import org.slf4j.Logger
import org.slf4j.LoggerFactory

object covidschema {


  /*
  val schema = new StructType()
    .add("location",StringType,true)
    .add("country_code",StringType,true)
    .add("latitude",LongType,true)
    .add("longitude",LongType,true)
    .add("confirmed",IntegerType,true)
    .add("dead",IntegerType,true)
    .add("recovered",IntegerType,true)
    .add("velocity_confirmed",IntegerType,true)
    .add("velocity_dead",IntegerType,true)
    .add("velocity_recovered",IntegerType,true)
    .add("updated_date",StringType,true)
    .add("eventTimeLong",LongType,true)


   */

  val schema = new StructType()
    .add("confirmed",DataTypes.LongType, true)
    .add("country_code", DataTypes.StringType, true)
    .add("dead", DataTypes.LongType, true)
    .add("latitude", DataTypes.LongType, true)
    .add("location", DataTypes.StringType, true)
    .add("longitude", DataTypes.LongType, true)
    .add("recovered", DataTypes.LongType, true)
    .add("velocity_confirmed", DataTypes.LongType, true)
    .add("velocity_dead", DataTypes.LongType, true)
    .add("velocity_recovered", DataTypes.LongType, true)
    .add("updated_date", DataTypes.StringType, true)
    .add("eventTimeLong", DataTypes.LongType, true);

  val schema2 = StructType(Array(
    StructField("confirmed",DataTypes.LongType, true),
    StructField("country_code", DataTypes.StringType, true),
    StructField("dead", DataTypes.LongType, true),
    StructField("latitude", DataTypes.LongType, true),
    StructField("location", DataTypes.StringType, true),
    StructField("longitude", DataTypes.LongType, true),
    StructField("recovered", DataTypes.LongType, true),
    StructField("velocity_confirmed", DataTypes.LongType, true),
    StructField("velocity_dead", DataTypes.LongType, true),
    StructField("velocity_recovered", DataTypes.LongType, true),
    StructField("updated_date", DataTypes.StringType, true),
    StructField("eventTimeLong", DataTypes.LongType, true)
  ))


  val coviddata = """{"location":"Pilõezinhos","country_code":"br","latitude":-6.843131,"longitude":-35.53058,"confirmed":87,"dead":0,"recovered":"null","velocity_confirmed":5,"velocity_dead":0,"velocity_recovered":0,"updated_date":"2020-06-13 00:20:20.628942+00:00","eventTimeLong":1592007620000}"""
}
