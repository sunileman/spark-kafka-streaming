package com.cloudera.examples

import org.apache.spark.sql.types.{DataTypes, LongType, StringType, StructField, StructType}

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

  /*

  val schema = new StructType()
    .add("confirmed", DataTypes.LongType, true)
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
    StructField("confirmed", DataTypes.LongType, true),
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

*/

  val bingschema = new StructType()
    .add("ID",StringType,true)
    .add("Updated",StringType,true)
    .add("Confirmed",StringType,true)
    .add("ConfirmedChange",StringType,true)
    .add("Deaths",StringType,true)
    .add("DeathsChange",StringType,true)
    .add("Recovered",StringType,true)
    .add("RecoveredChange",StringType,true)
    .add("Latitude",StringType,true)
    .add("Longitude",StringType,true)
    .add("ISO2",StringType,true)
    .add("ISO3",StringType,true)
    .add("Country_Region",StringType,true)
    .add("AdminRegion1",StringType,true)
    .add("AdminRegion2",StringType,true)

  val travelschema = new StructType()
    .add("location",StringType,true)
    .add("data",StringType,true)
    .add("insert_date",LongType,true)


  val geoapidata="""{"city":"Athens","countryCode":"USA","postalCode":"45701-3544","countryName":"United States","lat":"39.30271","long":"-82.13068"}"""
  val coviddata = """{"location":"Pilõezinhos","country_code":"br","latitude":-6.843131,"longitude":-35.53058,"confirmed":87,"dead":0,"recovered":0,"velocity_confirmed":5,"velocity_dead":0,"velocity_recovered":0,"updated_date":"2020-06-13 00:20:20.628942+00:00","eventTimeLong":1592007620000}"""
  val covidreversegeo = """{"api_timestamp":"1607111512399","city":"Itapiratins","countryCode":"BRA","postalCode":"77718-000","latitude":"-8.38028","countryName":"Brazil","longitude":"-48.1073"}"""
  val bingcovidapi = """{"ID":"64442421","Updated":"09/17/2020","Confirmed":"505","ConfirmedChange":"39","Deaths":"0","DeathsChange":"0","Recovered":"209","RecoveredChange":"2","Latitude":"-30.93488","Longitude":"-64.28046","ISO2":"AR","ISO3":"ARG","Country_Region":"Argentina","AdminRegion1":"Cordoba","AdminRegion2":""}"""
  val covidjhapi = """[{"Latitude":-19.015438,"Longitude":29.154857,"Country":"Zimbabwe","Province":null,"Date":1608537600000,"Type":"Deaths","Count":322,"Difference":2.0,"Source":"John Hopkins","Country Latest":222},{"Latitude":-19.015438,"Longitude":29.154857,"Country":"Zimbabwe","Province":null,"Date":1608451200000,"Type":"Deaths","Count":320,"Difference":2.0,"Source":"John Hopkins","Country Latest":222}]"""
}
