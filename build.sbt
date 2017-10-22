name := "aerospike_perf_test"

version := "1.0-SNAPSHOT"

scalaVersion := "2.12.3"

libraryDependencies ++= Seq(
 "com.aerospike" % "aerospike-client" % "4.0.8",
 "io.netty" % "netty-all" % "4.1.16.Final",
 "com.typesafe" % "config" % "1.3.1"
)

assemblyJarName in assembly := "aerospike_128_async_v2.jar"