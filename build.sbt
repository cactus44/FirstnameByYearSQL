name := "FirstnameByYearSQL"

version := "1.0"

scalaVersion := "2.11.7"

val sparkVersion = "2.1.0"

//resolvers ++= Seq("apache-snapshots" at "http://repository.apache.org/snapshots")

libraryDependencies ++= Seq("org.apache.spark" %% "spark-core" % "2.1.0",
  "org.apache.spark" %% "spark-sql" % "2.1.0",
  "org.apache.spark" %% "spark-streaming" % "2.1.0" )

//libraryDependencies += "org.apache.spark" % "spark-streaming_2.11"  % "2.1.0"


