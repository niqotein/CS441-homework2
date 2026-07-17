package com

import org.scalatest.funsuite.AnyFunSuite
import io.delta.tables.DeltaTable

/**
 * Pipeline stage: DELTA LAKE UPSERT (`MERGE`).
 *
 * Integration test for the core "upsert" capability of HW2's incremental logic:
 * `Main`'s doc-metadata and chunk tables are both kept fresh via the same
 * `whenMatched(...).updateAll().whenNotMatched().insertAll()` MERGE pattern. This
 * verifies that pattern actually updates an existing row in place — the mechanism
 * that keeps the index correct when a document's content changes.
 */
class DeltaMergeSpec extends AnyFunSuite {

  private val spark = TestSparkSession.spark

  test("Delta MERGE updates an existing doc's contentHash when its docId matches but hash differs") {
    val tmpPath = "./target/test-doc-table"
    import spark.implicits._

    // --- State 1: Write initial version of "docA" ---
    val df1 = Seq(("docA", "hash1")).toDF("docId", "contentHash")
    df1.write.format("delta").mode("overwrite").save(tmpPath)

    // --- State 2: A new run detects "docA" has a *new* hash ---
    val df2 = Seq(("docA", "hash2")).toDF("docId", "contentHash")

    // Perform the MERGE operation (as seen in Main.scala's Step 6)
    val deltaTable = DeltaTable.forPath(spark, tmpPath)
    deltaTable.alias("t")
      .merge(df2.alias("s"), "t.docId = s.docId")
      .whenMatched("t.contentHash <> s.contentHash").updateAll()
      .whenNotMatched().insertAll()
      .execute()

    val out = spark.read.format("delta").load(tmpPath).collect().head
    assert(out.getAs[String]("contentHash") == "hash2")
  }

  test("Delta MERGE inserts a brand-new docId rather than requiring a prior row") {
    val tmpPath = "./target/test-doc-table-insert"
    import spark.implicits._

    val existing = Seq(("docA", "hash1")).toDF("docId", "contentHash")
    existing.write.format("delta").mode("overwrite").save(tmpPath)

    val incoming = Seq(("docA", "hash1"), ("docB", "hash9")).toDF("docId", "contentHash")

    val deltaTable = DeltaTable.forPath(spark, tmpPath)
    deltaTable.alias("t")
      .merge(incoming.alias("s"), "t.docId = s.docId")
      .whenMatched("t.contentHash <> s.contentHash").updateAll()
      .whenNotMatched().insertAll()
      .execute()

    val rows = spark.read.format("delta").load(tmpPath).collect()
    assert(rows.length == 2, "Expected the pre-existing docA row plus a newly inserted docB row")
    assert(rows.exists(r => r.getAs[String]("docId") == "docB" && r.getAs[String]("contentHash") == "hash9"))
  }
}
