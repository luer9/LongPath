name := "LongPath"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.0",
  "org.apache.spark" %% "spark-sql" % "2.4.0"
)
libraryDependencies += "org.xerial.snappy" % "snappy-java" % "1.1.8.4" % "test"
libraryDependencies += "org.apache.poi" % "poi" % "5.0.0"
libraryDependencies += "org.apache.poi" % "poi-ooxml" % "5.0.0"