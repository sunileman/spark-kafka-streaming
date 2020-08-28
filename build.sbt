name := "spark-kafka-streaming"

version := "1.0"

scalaVersion := "2.11.12"

val sparkVersion = "2.4.5"

libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-library" % scalaVersion.value,

  //uncomment before packaging. comment during testing
  //"org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  //"org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion % "provided",
  //"org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  //"org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  //"org.apache.spark" %% "spark-streaming" % sparkVersion % "provided"

  //uncomment during testing, comment out prior to packaging
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion ,
  "org.apache.spark" %% "spark-core" % sparkVersion ,
  "org.apache.spark" %% "spark-sql" % sparkVersion ,
  "org.apache.spark" %% "spark-streaming" % sparkVersion
)