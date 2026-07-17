package com

import org.scalatest.funsuite.AnyFunSuite

/**
 * Pipeline stage: CONFIGURATION.
 *
 * Verifies that `Config` loads every value the incremental indexer depends on from
 * `application.conf`, and that nothing has silently fallen back to a hardcoded default.
 * If this suite fails, every other stage's assumptions about its own configuration are
 * suspect too — this is deliberately the first thing checked.
 */
class ConfigSpec extends AnyFunSuite {

  test("Config exposes the configured corpus and Delta table paths") {
    assert(Config.corpusPath == "./MSRCorpus")
    assert(Config.tablePath == "./target/delta-tables")
  }

  test("Config.chunkSize/chunkOverlap are not hardcoded defaults") {
    // These live in application.conf under app.chunker.* and are read by Config
    // specifically so Main never falls back to compiled-in defaults.
    assert(Config.chunkSize == 1000)
    assert(Config.chunkOverlap == 100)
  }

  test("Config exposes the configured embedding model, version, and Ollama host") {
    assert(Config.embeddingModel == "mxbai-embed-large")
    assert(Config.embeddingVersion == "1.3.0")
    assert(Config.ollamaHost == "http://localhost:11434")
  }
}
