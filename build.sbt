name := "spark-kafka-streaming"

version := "1.0"

scalaVersion := "2.11.12"
//scalaVersion := "2.12.8"


val sparkVersion = "2.4.5"
val phoenixVersion = "5.0.0.7.2.2.1-5"


resolvers += "cloudera.repo" at "https://cloudera-build-us-west-1.vpc.cloudera.com/s3/build/1377805/cdh/7.x/maven-repository/"
resolvers += "hortonworks.public.repo" at "https://repo.hortonworks.com/content/repositories/releases/"
resolvers += "hortonworks.repo" at "https://nexus-private.hortonworks.com/nexus/content/groups/public/"

libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-library" % scalaVersion.value,

  //"org.apache.phoenix" % "phoenix-spark" % "5.0.0.7.2.6.0-71",




  //uncomment before packaging. comment during testing




  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming" % sparkVersion% "provided",

  //the assuming here is that user will provide the jars. If you do not have the phoenix jars remove the provided statement
  "org.apache.phoenix" % "phoenix-spark" % phoenixVersion% "provided",
  "org.apache.phoenix" % "phoenix-client" % phoenixVersion% "provided",







  //uncomment during testing, comment out prior to packaging



  /*

  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion ,
  "org.apache.spark" %% "spark-core" % sparkVersion ,
  "org.apache.spark" %% "spark-sql" % sparkVersion ,
  "org.apache.spark" %% "spark-streaming" % sparkVersion


   */













)