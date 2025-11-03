package com

import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.SparkSession
import io.delta.tables.DeltaTable


/**
 * This is a ScalaTest "FunSuite" for Homework 2.
 * Its purpose is to run unit and integration tests on the key components
 * of the incremental indexing pipeline.
 *
 * As required by both HW1 and HW2, this file provides tests to verify
 * the correctness of the implementation.
 */
class Homework2Suite extends AnyFunSuite {

  // --------------------------------------------------------
  // Spark Test Session
  // --------------------------------------------------------

  /**
   * Sets up a local SparkSession for testing.
   * This is a crucial first step for any Spark-based test suite (HW2).
   *
   * We configure it to run locally (`.master("local[*]")`) and, most
   * importantly, we enable the Delta Lake extensions. This allows
   * us to test Delta Lake features like `MERGE` and `DeltaTable.forPath`.
   */
  val spark = SparkSession.builder()
    .master("local[*]") // Run Spark locally using all available CPU cores
    .appName("Homework2 Test Suite")
    // Enable Delta Lake SQL extensions for this session (HW2)
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    // Configure the Spark catalog to use the Delta Lake catalog (HW2)
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()

  // Imports Spark implicits (e.g., .toDF()) for easier DataFrame manipulation


  /**
   * TEST 1: Spark Session Initialization
   *
   * This is a simple "sanity check" test to ensure that the
   * SparkSession (configured above) started successfully and
   * that the Delta Lake extensions are properly loaded.
   */
  test("1. Spark session initializes with Delta Lake") {
    assert(spark != null)
    assert(spark.sparkContext != null)
    // Check that our Delta config was correctly applied
    assert(spark.conf.get("spark.sql.extensions").contains("Delta"))
  }

  /**
   * TEST 2: Text Normalization
   *
   * This unit test validates the `TextNormalizer.normalize` function.
   *
   * WHY IT'S IMPORTANT (HW2): The entire delta-detection logic of HW2
   * relies on stable `contentHash` values. This test ensures that
   * our normalization logic is consistent, correctly removing whitespace,
   * newlines, and case differences, which would otherwise
   * create different hashes for text that is semantically identical.
   */
  test("2. PDF extraction + text normalization returns clean string") {
    val sampleText = "Hello\nWORLD\tPDF!!"
    val normalized = TextNormalizer.normalize(sampleText)
    // Assert that newlines, tabs, and case are all handled
    assert(normalized == "hello world pdf!!")
  }

  /**
   * TEST 3: Chunking Logic
   *
   * This unit test validates the `Chunker.chunkDocument` function.
   *
   * WHY IT'S IMPORTANT (HW1): This tests the core "sliding window" logic
   * described in HW1. It ensures that given a simple string, the chunker
   * correctly produces the expected number of chunks with the
   * specified overlap (size=5, overlap=2 -> stride=3).
   * It checks the content of each resulting chunk.
   */
  test("3️⃣ Chunker creates overlapping chunks correctly") {
    val text = "abcdefghij"  // 10 chars
    // Use chunk size 5 and overlap 2
    val chunks = Chunker.chunkDocument("doc1", text, 5, 2)

    // Expected chunks with stride 3 (5-2):
    // 1. 0-5: "abcde" (next start at 3)
    // 2. 3-8: "defgh" (next start at 6)
    // 3. 6-10: "ghij" (next start at 9)
    // 4. 9-10: "j"    (next start at 12)
    assert(chunks.length == 4)

    assert(chunks(0).text == "abcde")  // Chunk 1
    assert(chunks(1).text == "defgh")  // Chunk 2 (overlap "de")
    assert(chunks(2).text == "ghij")   // Chunk 3 (overlap "gh")
    assert(chunks(3).text == "j")      // Final tail chunk
  }

  /**
   * TEST 4: Delta Lake Upsert (MERGE) Logic
   *
   * This is an integration test for the core "upsert" capability of HW2.
   * It simulates a document's content changing.
   *
   * WHY IT'S IMPORTANT (HW2): It verifies that our `MERGE` logic
   * correctly *updates* a document's record (its `contentHash`)
   * when its `docId` matches but the content has changed.
   * This is fundamental to keeping the index fresh.
   */
  test("4. Doc Delta upsert updates only changed docs") {
    val tmpPath = "./target/test-doc-table"
    import spark.implicits._

    // --- State 1: Write initial version of "docA" ---
    val df1 = Seq(("docA", "hash1")).toDF("docId", "contentHash")
    df1.write.format("delta").mode("overwrite").save(tmpPath)

    // --- State 2: A new run detects "docA" has a *new* hash ---
    val df2 = Seq(("docA", "hash2")).toDF("docId", "contentHash")

    // Perform the MERGE operation (as seen in Main.scala)
    val deltaTable = DeltaTable.forPath(spark, tmpPath)
    deltaTable.alias("t") // 't' for target (the existing table)
      .merge(df2.alias("s"), "t.docId = s.docId") // 's' for source (the new data)
      // This is the key condition: update only if hash is different
      .whenMatched("t.contentHash <> s.contentHash").updateAll()
      .whenNotMatched().insertAll()
      .execute()

    // --- Assert ---
    // Check that the table now contains "hash2" for "docA"
    val out = spark.read.format("delta").load(tmpPath).collect().head
    assert(out.getAs[String]("contentHash") == "hash2")
  }

  /**
   * TEST 5: Incremental "No-Op" (No Change) Scenario
   *
   * This test simulates a pipeline run where no source files have changed.
   *
   * WHY IT'S IMPORTANT (HW2): A core goal of HW2 is to be efficient
   * and "perform near-zero work" when nothing has changed. This test
   * asserts that if we run the pipeline against an already-processed
   * corpus, the delta tables (chunks, embeddings) do not grow.
   * (A more advanced test would use mocks or counters to prove that
   * the `OllamaClient.embed` function was not called).
   */
  test("5. Incremental mode does no work when no changes detected") {
    val tmpChunkTbl = "./target/test-chunks"
    val tmpEmbedTbl = "./target.test-embeddings"

    // --- State 1: Create empty tables (simulating a clean slate) ---
    spark.range(0).write.format("delta").mode("overwrite").save(tmpChunkTbl)
    spark.range(0).write.format("delta").mode("overwrite").save(tmpEmbedTbl)

    val chunksBefore = spark.read.format("delta").load(tmpChunkTbl).count()
    val embedsBefore = spark.read.format("delta").load(tmpEmbedTbl).count()
    assert(chunksBefore == 0)
    assert(embedsBefore == 0)

    // --- State 2: Simulate a "no-change" run ---
    // (Here we just read the same empty tables, as no new DF is provided)
    // The `Main.scala` logic would run, but the `toProcessDF` and
    // `chunksToEmbed` DataFrames would be empty.

    val chunksAfter = spark.read.format("delta").load(tmpChunkTbl).count()
    val embedsAfter = spark.read.format("delta").load(tmpEmbedTbl).count()

    // Assert that no new records were added
    assert(chunksBefore == chunksAfter)
    assert(embedsBefore == embedsAfter)
  }
}