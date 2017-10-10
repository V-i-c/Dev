name := "aerospike_perf_test"

version := "1.0-SNAPSHOT"

scalaVersion := "2.12.3"

libraryDependencies ++= Seq(
 "com.aerospike" % "aerospike-client" % "4.0.8",
 "com.typesafe" % "config" % "1.3.1"
)

assemblyJarName in assembly := "aerospike_benchmark_1024_sync.jar"