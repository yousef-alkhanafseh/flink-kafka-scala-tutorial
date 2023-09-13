ThisBuild / resolvers ++= Seq(
  "Apache Development Snapshot Repository" at "https://repository.apache.org/content/repositories/snapshots/",
  Resolver.mavenLocal
)

crossPaths := false

name := "Flink Project"

version := "0.1-SNAPSHOT"

organization := "org.example"
ThisBuild / scalaVersion := "2.12.8"

val flinkVersion = "1.16.0"

libraryDependencies += "org.apache.flink" %% "flink-scala" % flinkVersion % "provided"
libraryDependencies += "org.apache.flink" %% "flink-streaming-scala" % flinkVersion % "provided"
libraryDependencies += "org.apache.flink" % "flink-connector-kafka" % flinkVersion % "provided"
libraryDependencies += "org.apache.flink" % "flink-core" % flinkVersion % "provided"

libraryDependencies += "org.apache.kafka" % "kafka-clients" % "3.5.1"
libraryDependencies += "org.apache.flink" % "flink-clients" % "1.16.0"

mainClass in Compile := Some("org.apache.flink.flinkKafkaScalaTutorial")
