name := "scala-spark"

version := "1.0"
scalaVersion := "2.12.17"

val sparkVersion = "3.2.1"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
