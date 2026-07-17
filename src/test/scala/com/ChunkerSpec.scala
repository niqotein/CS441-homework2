package com

import org.scalatest.funsuite.AnyFunSuite

/**
 * Pipeline stage: CHUNKING (`Chunker.chunkDocument`).
 *
 * Chunk boundaries and IDs are the foundation of HW2's delta logic: `chunkId` must be
 * stable across runs (based on docId + offsets) so Delta MERGE can detect unchanged
 * chunks, and `contentHash` must actually reflect the chunk's text so a content change
 * is detected. This suite checks both the sliding-window geometry and those hashing
 * properties directly, in isolation from Spark/Delta.
 */
class ChunkerSpec extends AnyFunSuite {

  test("chunkDocument returns a single chunk when the text is shorter than chunkSize") {
    val text = "a short document"
    val chunks = Chunker.chunkDocument("doc1", text, chunkSize = 1000, chunkOverlap = 100)

    assert(chunks.length == 1)
    assert(chunks.head.text == text)
    assert(chunks.head.startOffset == 0)
    assert(chunks.head.endOffset == text.length)
  }

  test("chunkDocument creates overlapping chunks using the configured stride (size - overlap)") {
    val text = "abcdefghij" // 10 chars
    val chunks = Chunker.chunkDocument("doc1", text, chunkSize = 5, chunkOverlap = 2)

    // Stride = 5 - 2 = 3: starts at 0, 3, 6, 9.
    assert(chunks.length == 4)
    assert(chunks(0).text == "abcde")
    assert(chunks(1).text == "defgh") // overlaps "de" with chunk 0
    assert(chunks(2).text == "ghij")  // overlaps "gh" with chunk 1
    assert(chunks(3).text == "j")     // final tail chunk
  }

  test("chunkDocument returns no chunks for empty input") {
    assert(Chunker.chunkDocument("doc1", "", chunkSize = 1000, chunkOverlap = 100).isEmpty)
  }

  test("no chunk's text ever exceeds the configured chunkSize") {
    val text = "x" * 10000
    val chunks = Chunker.chunkDocument("doc1", text, chunkSize = 777, chunkOverlap = 50)

    assert(chunks.nonEmpty)
    assert(chunks.forall(_.text.length <= 777))
  }

  test("chunkId is deterministic: identical docId, offsets, and stride produce the same hash across calls") {
    val text = "the same document processed twice should be fully deterministic"
    val first = Chunker.chunkDocument("doc1", text, chunkSize = 20, chunkOverlap = 5)
    val second = Chunker.chunkDocument("doc1", text, chunkSize = 20, chunkOverlap = 5)

    assert(first.map(_.chunkId) == second.map(_.chunkId))
  }

  test("chunkId differs for the same offsets under a different docId") {
    val text = "identical text content, different parent document"
    val fromDoc1 = Chunker.chunkDocument("doc1", text, chunkSize = 20, chunkOverlap = 5)
    val fromDoc2 = Chunker.chunkDocument("doc2", text, chunkSize = 20, chunkOverlap = 5)

    // Same offsets/stride, but chunkId is docId-scoped, so it must not collide across docs.
    assert(fromDoc1.map(_.chunkId) != fromDoc2.map(_.chunkId))
  }

  test("contentHash reflects the chunk's own text, not its position") {
    val chunksA = Chunker.chunkDocument("doc1", "aaaaaaaaaa", chunkSize = 10, chunkOverlap = 0)
    val chunksB = Chunker.chunkDocument("doc1", "bbbbbbbbbb", chunkSize = 10, chunkOverlap = 0)

    assert(chunksA.head.contentHash != chunksB.head.contentHash)

    // Same docId, same offsets, same text -> same contentHash (and same chunkId).
    val chunksA2 = Chunker.chunkDocument("doc1", "aaaaaaaaaa", chunkSize = 10, chunkOverlap = 0)
    assert(chunksA.head.contentHash == chunksA2.head.contentHash)
  }
}
