lazy val root = (project in file(".")).
  settings(
    name := "scala-spark",
    version := "1.0",
    scalaVersion := "2.12.17",
    publishMavenStyle := true
  )

val dependencyScope = "provided"
val SparkVersion = "3.2.1"
val HadoopVersion = "3.3.1"
val SedonaVersion = "1.4.1"
val SparkCompatibleVersion = "3.0"
val ScalaCompatibleVersion = "2.12"

libraryDependencies += "org.apache.spark" %% "spark-core" % SparkVersion % dependencyScope exclude("org.apache.hadoop", "*")
libraryDependencies += "org.apache.spark" %% "spark-sql" % SparkVersion % dependencyScope exclude("org.apache.hadoop", "*")
libraryDependencies += "org.locationtech.jts" % "jts-core" % "1.18.2"
libraryDependencies += "org.apache.sedona" %% "sedona-spark-shaded-3.0" % SedonaVersion
libraryDependencies += "org.apache.sedona" %% "sedona-viz-3.0" % SedonaVersion
libraryDependencies += "org.datasyslab" % "geotools-wrapper" % "1.4.0-28.2"

assemblyMergeStrategy in assembly := {
  case PathList("org.apache.sedona", "sedona-core", xs@_*) => MergeStrategy.first
  case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
  case PathList("META-INF","services",xs @ _*) => MergeStrategy.filterDistinctLines
  case path if path.endsWith(".SF") => MergeStrategy.discard
  case path if path.endsWith(".DSA") => MergeStrategy.discard
  case path if path.endsWith(".RSA") => MergeStrategy.discard
  case _ => MergeStrategy.first
}
