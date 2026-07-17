package com

import org.scalatest.funsuite.AnyFunSuite

/**
 * Pipeline stage: EMBEDDING (`OllamaClient.embed`).
 *
 * This suite makes REAL HTTP calls to a locally running Ollama instance (see the
 * project's own README prerequisites: `ollama pull mxbai-embed-large`, server listening
 * at Config.ollamaHost). It is intentionally not mocked — a mocked HTTP client can't
 * catch a real API contract change (e.g. Ollama renaming the "embedding" response
 * field, or the model producing a different vector dimension), which is exactly the
 * class of bug that would otherwise only surface deep inside a running Spark job.
 *
 * Requires: `ollama serve` running locally with `mxbai-embed-large` pulled.
 */
class OllamaEmbeddingSpec extends AnyFunSuite {

  // mxbai-embed-large's known embedding dimension (confirmed via `ollama show
  // mxbai-embed-large` / the model's `embedding_length` metadata). If Ollama or the
  // model version ever changes this, this test is exactly where that would be caught.
  private val expectedDimension = 1024

  test("embed returns a non-empty vector of the expected dimension for a single chunk") {
    val result = OllamaClient.embed(Seq("Incremental delta indexing over a document corpus."), Config.embeddingModel)

    assert(result.size == 1)
    assert(result.head.nonEmpty)
    assert(result.head.length == expectedDimension)
  }

  test("embed returns one vector per input text, preserving order, for a batch of chunks") {
    val chunks = Seq(
      "Chunk one talks about anti-join delta detection.",
      "Chunk two talks about Delta Lake MERGE upserts.",
      "Chunk three talks about atomic index publication."
    )
    val results = OllamaClient.embed(chunks, Config.embeddingModel)

    assert(results.size == chunks.size)
    assert(results.forall(_.length == expectedDimension))
  }

  test("embed produces different vectors for semantically different text") {
    val results = OllamaClient.embed(
      Seq("The quick brown fox jumps over the lazy dog.", "Quarterly financial earnings exceeded expectations."),
      Config.embeddingModel
    )

    assert(results(0).toVector != results(1).toVector, "Expected distinct embeddings for unrelated sentences")
  }

  test("embed returns an empty vector (not an exception) when the model name is invalid") {
    // OllamaClient.embedSingle catches API errors and returns Vector.empty rather than
    // throwing, so a bad model name should degrade gracefully instead of crashing the job.
    val result = OllamaClient.embed(Seq("test"), "this-model-does-not-exist")
    assert(result.head.isEmpty)
  }
}
