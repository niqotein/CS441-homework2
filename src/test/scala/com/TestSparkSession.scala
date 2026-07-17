package com

import org.apache.spark.sql.SparkSession

/**
 * Shared local SparkSession (with Delta Lake extensions enabled) for the test suites
 * that need one. A JVM-wide singleton rather than a per-suite `val` so that splitting
 * the old monolithic suite into focused spec files doesn't pay Spark's ~5-10s local
 * session startup cost once per file.
 */
object TestSparkSession {
  lazy val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("Homework2 Test Suite")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
}
