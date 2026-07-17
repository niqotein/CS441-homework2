package com

import org.scalatest.funsuite.AnyFunSuite

/**
 * Pipeline stage: TEXT NORMALIZATION (`TextNormalizer.normalize`).
 *
 * The entire delta-detection logic of HW2 relies on stable `contentHash` values, which
 * are computed on normalized text. This suite ensures normalization is consistent and
 * idempotent — otherwise semantically identical text could hash differently across
 * runs and trigger spurious re-indexing.
 */
class TextNormalizerSpec extends AnyFunSuite {

  test("normalize collapses newlines, tabs, and multiple spaces into single spaces") {
    val sampleText = "Hello\nWORLD\tPDF!!"
    assert(TextNormalizer.normalize(sampleText) == "hello world pdf!!")
  }

  test("normalize trims leading and trailing whitespace") {
    assert(TextNormalizer.normalize("   padded text   ") == "padded text")
  }

  test("normalize is idempotent") {
    val once = TextNormalizer.normalize("Mixed \t Whitespace\n\nAnd CAPS")
    val twice = TextNormalizer.normalize(once)
    assert(once == twice)
  }

  test("normalize NFC-normalizes composite unicode characters so equivalent text hashes the same") {
    // "e" followed by a combining acute accent (U+0301) vs. the single precomposed
    // "é" code point render identically but are different byte sequences until
    // NFC-normalized. Since contentHash is computed on normalize()'s output, these
    // must collapse to the same string, or two visually-identical PDFs could hash
    // differently and confuse delta detection.
    val decomposed = "café"
    val precomposed = "café"

    assert(decomposed != precomposed, "test setup check: these must be different byte sequences to start with")
    assert(TextNormalizer.normalize(decomposed) == TextNormalizer.normalize(precomposed))
  }
}
