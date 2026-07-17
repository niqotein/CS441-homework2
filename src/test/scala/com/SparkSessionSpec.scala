package com

import org.scalatest.funsuite.AnyFunSuite

/**
 * Pipeline stage: SPARK/DELTA SESSION BOOTSTRAP.
 *
 * A sanity check that the shared local SparkSession (see `TestSparkSession`) actually
 * starts and has the Delta Lake SQL extensions loaded. Every other Spark-backed spec in
 * this suite depends on this being true; if this fails, their failures point at the
 * environment, not at pipeline logic.
 */
class SparkSessionSpec extends AnyFunSuite {

  test("Spark session initializes with Delta Lake extensions") {
    val spark = TestSparkSession.spark

    assert(spark != null)
    assert(spark.sparkContext != null)
    assert(spark.conf.get("spark.sql.extensions").contains("Delta"))
  }
}
