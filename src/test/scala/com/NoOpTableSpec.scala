package com

import org.scalatest.funsuite.AnyFunSuite

/**
 * Pipeline stage: IDEMPOTENT NO-OP BEHAVIOR (partial coverage — see below).
 *
 * NOTE ON SCOPE: despite what a test like this might imply, it does not invoke `Main`'s
 * actual pipeline logic at all — it only creates two empty Delta tables and asserts
 * their row counts stay at 0. It does not prove that a real no-op rerun of `Main` skips
 * embedding or leaves the tables unchanged; it only confirms Delta reads/writes behave
 * as expected on empty tables. A stronger version of this test would run `Main`'s
 * pipeline twice against a fixed corpus and assert that `OllamaClient.embed` is not
 * invoked (e.g. via a mock/counter) on the second run — the true idempotency guarantee
 * is currently verified manually (see README's "Known Limitations" section), not by
 * this automated suite.
 */
class NoOpTableSpec extends AnyFunSuite {

  private val spark = TestSparkSession.spark

  test("Empty Delta tables report zero rows (not an end-to-end idempotency check)") {
    val tmpChunkTbl = "./target/test-chunks"
    val tmpEmbedTbl = "./target/test-embeddings"

    spark.range(0).write.format("delta").mode("overwrite").save(tmpChunkTbl)
    spark.range(0).write.format("delta").mode("overwrite").save(tmpEmbedTbl)

    val chunksBefore = spark.read.format("delta").load(tmpChunkTbl).count()
    val embedsBefore = spark.read.format("delta").load(tmpEmbedTbl).count()
    assert(chunksBefore == 0)
    assert(embedsBefore == 0)

    val chunksAfter = spark.read.format("delta").load(tmpChunkTbl).count()
    val embedsAfter = spark.read.format("delta").load(tmpEmbedTbl).count()

    assert(chunksBefore == chunksAfter)
    assert(embedsBefore == embedsAfter)
  }
}
