package com

import java.nio.file.{Files, Paths}
import org.scalatest.funsuite.AnyFunSuite

/**
 * Pipeline stage: PDF TEXT EXTRACTION (`PdfProcessor.extractText`).
 *
 * Runs against a real sample PDF from the project's own `MSRCorpus/` directory rather
 * than a synthetic byte array, so a real PDFBox parsing failure (corrupt file, unsupported
 * encoding, empty output) shows up here instead of surfacing for the first time inside a
 * live Spark UDF.
 */
class PdfExtractionSpec extends AnyFunSuite {

  // The smallest sample PDF in the corpus, kept small so this test stays fast.
  private val samplePdfPath = Paths.get(Config.corpusPath, "1083142.1083144.pdf")

  test("PdfProcessor.extractText extracts non-empty, readable text from a real sample PDF") {
    assert(Files.exists(samplePdfPath), s"Expected sample PDF at $samplePdfPath — is MSRCorpus/ present?")

    val bytes = Files.readAllBytes(samplePdfPath)
    val text = PdfProcessor.extractText(bytes)

    assert(text.nonEmpty, "Expected extracted text to be non-empty")
    // A real MSR conference paper should yield at least a few hundred characters of body text.
    assert(text.length > 200, s"Extracted text looked suspiciously short (${text.length} chars)")
  }

  test("PdfProcessor.extractText output is safe to feed into TextNormalizer.normalize") {
    val bytes = Files.readAllBytes(samplePdfPath)
    val text = PdfProcessor.extractText(bytes)

    // This is exactly what Main's ingest UDFs do next in the real pipeline: normalize
    // whatever PDFBox extracted before hashing/chunking it.
    val normalized = TextNormalizer.normalize(text)
    assert(normalized.nonEmpty)
    assert(!normalized.contains("\n"), "normalize should collapse all whitespace, including newlines")
  }

  test("PdfProcessor.extractText returns an empty string (not an exception) for invalid PDF bytes") {
    // Mirrors what a corrupt/non-PDF file in the corpus would produce; Main's ingest
    // pipeline filters rows where text == "" rather than letting the job crash on one bad file.
    val result = PdfProcessor.extractText("this is not a PDF".getBytes("UTF-8"))
    assert(result == "")
  }
}
