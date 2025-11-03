// Sets the version for your own project
ThisBuild / version := "0.1.0-SNAPSHOT"

// Sets the Scala version. HW1 requires Scala.
// Note: HW1 pseudocode mentions Scala 3.5.1, but your project is set to 2.13.14.
// Spark 3.4.1 (which you use below) officially supports Scala 2.13, so 2.13.14 is a good choice.
ThisBuild / scalaVersion := "2.13.14"

// Define Spark and Delta versions as variables for easy updates
val sparkVersion = "3.4.1"
val deltaVersion = "2.4.0"

lazy val root = (project in file("."))
  .settings(
    name := "441homework2"
  )

// This block defines all external libraries your project depends on.
libraryDependencies ++= Seq(

  // --- Spark and Delta Dependencies (HW2) ---

  // Spark SQL: The entry point for Spark, providing DataFrame and Dataset APIs.
  // This is the core of your "Spark computational model" for HW2.
  "org.apache.spark" %% "spark-sql" % sparkVersion exclude("org.apache.logging.log4j", "log4j-slf4j2-impl"),

  // Delta Lake Core: Adds ACID transactions, schema enforcement, and the
  // powerful `MERGE` (upsert) command to Spark. This is essential
  // for the incremental delta logic in HW2.
  "io.delta" %% "delta-core" % deltaVersion,

  // --- PDF and RAG Dependencies (HW1) ---

  // Apache PDFBox: Used by your `PdfProcessor` to extract raw text
  // from the MSRCorpus PDF files[cite: 1, 2].
  "org.apache.pdfbox" % "pdfbox" % "2.0.29",

  // Apache Lucene: The library used to build the vector index (HNSW)
  // for similarity search in the RAG pipeline[cite: 1, 2].
  "org.apache.lucene" % "lucene-core" % "9.9.1",

  // --- Utility Dependencies (HW1) ---

  // Logback: The logging implementation, as required by HW1[cite: 1, 2].
  "ch.qos.logback" % "logback-classic" % "1.5.6",

  // Scala Logging: A convenient Scala wrapper (SLF4J) for logging.
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5",

  // Typesafe Config: The library for loading `application.conf`[cite: 1, 2].
  // Required by HW1 to avoid hardcoding values.
  "com.typesafe" % "config" % "1.4.3",

  // --- Ollama HTTP Client Dependencies (HW1) ---

  // STTP Core: A functional HTTP client library for Scala,
  // used to call the Ollama API[cite: 2].
  "com.softwaremill.sttp.client3" %% "core" % "3.9.5",

  // STTP Circe JSON: Integrates STTP with the Circe library for
  // automatic JSON parsing of Ollama's requests and responses[cite: 1, 2].
  "com.softwaremill.sttp.client3" %% "circe" % "3.9.5",

  // Circe Generic: Provides automatic derivation of JSON encoders/decoders
  // for your case classes (e.g., `SingleEmbedRequest`)[cite: 1, 2].
  "io.circe" %% "circe-generic" % "0.14.9",

  // Circe Parser: The JSON parsing library[cite: 1, 2].
  "io.circe" %% "circe-parser" % "0.14.9",

  // STTP OkHttp Backend: The actual synchronous HTTP client implementation
  // that sends the requests[cite: 1, 2].
  "com.softwaremill.sttp.client3" %% "okhttp-backend" % "3.9.0",

  // --- Testing Dependencies (HW1/HW2) ---

  // ScalaTest: The testing framework used for your unit tests
  // (e.g., `Homework2Suite.scala`).
  // Both homeworks require tests.
  "org.scalatest" %% "scalatest" % "3.2.18" % "test",
)

// --- Dependency Resolution ---

// This forces a specific version of `jackson-databind` to be used.
// It's often necessary in Spark projects to resolve "classpath hell"
// where different libraries (like Spark and Delta) depend on
// conflicting versions of the same library.
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.14.2"

// This tells SBT where to find the Delta Lake artifacts.
resolvers += "Delta Lake" at "https://repo1.maven.org/maven2"