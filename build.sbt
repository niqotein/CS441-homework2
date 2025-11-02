ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.13.14"

val sparkVersion = "3.4.1"
val deltaVersion = "2.4.0"

lazy val root = (project in file("."))
  .settings(
    name := "441homework2"
  )
libraryDependencies ++= Seq(
  //  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion exclude("org.apache.logging.log4j", "log4j-slf4j2-impl"),
  "io.delta" %% "delta-core" % deltaVersion,

  // PDF extraction
  "org.apache.pdfbox" % "pdfbox" % "2.0.29",

  // Logging
  "ch.qos.logback" % "logback-classic" % "1.5.6",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5",

  // Configuration
  "com.typesafe" % "config" % "1.4.3",

  // HTTP Client (for Ollama) and JSON parsing
  "com.softwaremill.sttp.client3" %% "core" % "3.9.5",
  "com.softwaremill.sttp.client3" %% "circe" % "3.9.5",
  "io.circe" %% "circe-generic" % "0.14.9",
  "io.circe" %% "circe-parser" % "0.14.9",
  "com.softwaremill.sttp.client3" %% "okhttp-backend" % "3.9.0",

  // Add ScalaTest for unit and integration testing
  "org.scalatest" %% "scalatest" % "3.2.18" % "test",

  "org.apache.lucene" % "lucene-core" % "9.9.1"
)

dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.14.2"

resolvers += "Delta Lake" at "https://repo1.maven.org/maven2"