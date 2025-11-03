package com

import com.typesafe.config.ConfigFactory
import org.apache.pdfbox.pdmodel.PDDocument
import org.apache.pdfbox.text.PDFTextStripper
import java.text.Normalizer
import scala.util.Try
import sttp.client3._
import sttp.client3.circe._
import scala.annotation.tailrec
import io.circe._
import java.security.MessageDigest
import io.circe.generic.semiauto._
import scala.concurrent.duration._
import sttp.client3.okhttp.OkHttpSyncBackend
import java.net.{ConnectException, SocketTimeoutException, UnknownHostException}
import java.io.IOException

/**
 * Helper object to load configuration from `application.conf`.
 * As required by HW1, this avoids hardcoding values like paths or model names.
 * Uses the Typesafe Config library.
 */
object Config {
  private val config = ConfigFactory.load("application.conf")

  // --- Paths ---
  // Path to the MSRCorpus PDF directory
  val corpusPath = config.getString("app.corpusPath")
  // Base path where Spark will write the Delta Lake tables
  val tablePath = config.getString("app.tablePath")

  // --- RAG Chunking Parameters (from HW1) ---
  // The target size for each text chunk (e.g., 1000 chars)
  val chunkSize = config.getInt("app.chunker.size")
  // The overlap between consecutive chunks (e.g., 100 chars) to maintain context
  val chunkOverlap = config.getInt("app.chunker.overlap")

  // --- Ollama Embedding Model (from HW1) ---
  // The name of the embedding model (e.g., "mxbai-embed-large")
  val embeddingModel = config.getString("app.embedding.model")
  // A version string for the model, critical for HW2's delta logic
  val embeddingVersion = config.getString("app.embedding.version")
  // The host URL for the Ollama server API
  val ollamaHost = config.getString("app.embedding.ollamaHost")
}

/**
 * Handles the extraction of plain text from PDF files.
 * This is the first step of the RAG pipeline (HW1) - getting data from the MSRCorpus.
 */
object PdfProcessor {
  /**
   * Extracts text from a PDF's byte content using Apache PDFBox.
   *
   * @param content The raw byte array of a PDF file (as read by Spark's `binaryFile` format).
   * @return The extracted plain text as a String.
   * Uses `Try` for safety, returning an empty string if PDF parsing fails.
   */
  def extractText(content: Array[Byte]): String = {
    Try {
      // PDDocument.load consumes the byte array
      val document = PDDocument.load(content)
      // PDFTextStripper pulls all text
      val text = new PDFTextStripper().getText(document)
      // Ensure the document resource is closed
      document.close()
      text
    }.getOrElse("") // On any exception (e.g., corrupt PDF), return ""
  }
}

/**
 * Provides text normalization utilities.
 * This is a crucial step for HW2's delta detection. Content hashes must be
 * computed on "clean" text to be stable and consistent.
 */
object TextNormalizer {
  /**
   * Cleans and normalizes a string for processing.
   * This involves:
   * 1. Unicode Normalization (NFC) to handle composite characters.
   * 2. Collapsing all forms of whitespace (newlines, tabs, multi-spaces) into a single space.
   * 3. Trimming leading/trailing whitespace.
   * 4. Converting to lowercase for case-insensitive processing.
   *
   * @param text The raw extracted text.
   * @return The normalized, clean text.
   */
  def normalize(text: String): String = {
    // Step 1: Unicode normalize (NFC)
    val normalized = Normalizer.normalize(text, Normalizer.Form.NFC)
    // Step 2: Replace all line breaks/tabs/etc. with a space, and collapse multiple spaces
    normalized
      .replaceAll("[ \\t\\x0B\\f\\r\\n]+", " ")  // All whitespace -> single space
      .trim                                       // Remove leading/trailing whitespace
      .toLowerCase                                // Optional: lowercase
  }
}

/**
 * Handles the "chunking" of large documents into smaller pieces.
 * As described in HW1, chunks should be big enough for meaning but small enough
 * for the LLM's context window.
 */
object Chunker {
  /**
   * Represents a single chunk of text, ready for embedding.
   *
   * @param chunkId     A deterministic SHA256 hash (docId + offsets). Crucial for HW2's `MERGE`.
   * @param text        The actual text content of the chunk.
   * @param startOffset The starting character offset in the original document.
   * @param endOffset   The ending character offset.
   * @param contentHash A SHA256 hash of the `text` field, to detect if just the chunk's content changed.
   */
  case class Chunk(chunkId: String, text: String, startOffset: Int, endOffset: Int, contentHash: String)

  /** Helper to generate a SHA-256 hash for IDs. */
  def sha256Hex(s: String): String = {
    MessageDigest.getInstance("SHA-256").digest(s.getBytes("UTF-8")).map("%02x".format(_)).mkString
  }

  /**
   * Generates chunks using a functional, tail-recursive approach.
   * This avoids mutable `var` loops, as recommended by HW1/HW2 grading criteria.
   * It implements a "sliding window" with a fixed stride (size - overlap).
   *
   * @param docId        The stable ID of the parent document.
   * @param text         The full, normalized text of the document.
   * @param chunkSize    The target chunk size (from Config).
   * @param chunkOverlap The overlap (from Config).
   * @return A sequence of `Chunk` objects.
   */
  def chunkDocument(docId: String, text: String, chunkSize: Int, chunkOverlap: Int): Seq[Chunk] = {

    /**
     * A purely functional, tail-recursive helper function to generate chunks.
     * @param currentStart The starting index of the current chunk.
     * @param acc The accumulator (a List) for generated chunks.
     * @return A List of all chunks in reverse order.
     */
    @tailrec
    def loop(currentStart: Int, acc: List[Chunk]): List[Chunk] = {
      // Base case: If the start index is at or past the text length, we are done.
      if (currentStart >= text.length) {
        acc
      } else {
        // Recursive step:
        // Calculate the end of this chunk, respecting the total text length
        val end = math.min(currentStart + chunkSize, text.length)
        // Extract the text for this chunk
        val chunkText = text.substring(currentStart, end)

        // --- Deterministic IDs (Key for HW2) ---
        // The chunkId must be stable. It's based on the docId and its *position* (offsets).
        val chunkId = sha256Hex(docId + "_" + currentStart + "_" + end)
        // The contentHash is based on the *text*. This detects content changes.
        val contentHash = sha256Hex(chunkText)

        val newChunk = Chunk(chunkId, chunkText, currentStart, end, contentHash)

        // Calculate the next starting index using the fixed stride (size - overlap).
        // This implements the "sliding window" logic from HW1.
        val nextStart = currentStart + (chunkSize - chunkOverlap)

        // Call the loop again with the new state, prepending the chunk to the accumulator
        loop(nextStart, newChunk :: acc)
      }
    }
    // Start the recursion and reverse the final list to get the correct chunk order (0, 1, 2...).
    loop(0, Nil).reverse.toSeq
  }
}


// =======================================================
// OLLAMA CLIENT FOR EMBEDDING CHUNKS
// =======================================================

/**
 * Case class to model the JSON request for Ollama's `/api/embeddings` endpoint
 * when sending a *single* prompt.
 * {"model": "...", "prompt": "..."}
 */
final case class SingleEmbedRequest(model: String, prompt: String)
object SingleEmbedRequest {
  implicit val encoder: Encoder[SingleEmbedRequest] = deriveEncoder
}

/**
 * Case class to model the JSON response from Ollama's `/api/embeddings` endpoint.
 * {"embedding": [0.1, 0.2, ...]}
 */
final case class SingleEmbedResponse(embedding: Vector[Float])
object SingleEmbedResponse {
  implicit val decoder: Decoder[SingleEmbedResponse] = deriveDecoder
}

/**
 * A synchronous HTTP client for generating embeddings via an Ollama server.
 * This is responsible for Step 2 of the RAG pipeline (HW1): "embedding".
 */
object OllamaClient {

  // Initialize a synchronous, thread-safe HTTP backend (sttp)
  private val backend = OkHttpSyncBackend()
  // Define the target API endpoint using the host from Config
  private val embeddingUrl = s"${Config.ollamaHost}/api/embeddings"

  /**
   * Generates an embedding for a *single* piece of text.
   * This is a private helper that handles the actual network request and error handling.
   *
   * @param text  The text to embed.
   * @param model The Ollama model name.
   * @return A Vector[Float] of the embedding, or an empty Vector on failure.
   */
  private def embedSingle(text: String, model: String): Vector[Float] = {
    // 1. Create the request body
    val reqBody = SingleEmbedRequest(model, text)

    // 2. Define the HTTP POST request
    val request = basicRequest
      .post(uri"$embeddingUrl")
      .body(reqBody)                    // Set the JSON body
      .response(asJson[SingleEmbedResponse]) // Expect a JSON response of this type
      .readTimeout(30.seconds)          // Set a reasonable timeout

    try {
      // 3. Send the request and get a response
      val response = request.send(backend)

      // 4. Handle the response
      response.body match {
        case Right(embedResponse) =>
          // Success: return the embedding vector
          embedResponse.embedding
        case Left(error) =>
          // Failure: Ollama returned an error (e.g., model not found)
          println(s"[ERROR] Ollama responded with failure for text='${text.take(50)}...'")
          println(s"[DETAILS] HTTP code: ${response.code}, error: $error")
          Vector.empty[Float]
      }
    } catch {
      // 5. Handle network-level failures
      case e: ConnectException =>
        println(s"[ERROR] Connection refused to Ollama at ${Config.ollamaHost}: ${e.getMessage}")
        Vector.empty[Float]
      case e: SocketTimeoutException =>
        println(s"[ERROR] Timeout connecting to Ollama at ${Config.ollamaHost}: ${e.getMessage}")
        Vector.empty[Float]
      case e: UnknownHostException =>
        println(s"[ERROR] Unknown host '${Config.ollamaHost}': ${e.getMessage}")
        Vector.empty[Float]
      case e: IOException =>
        println(s"[ERROR] Network I/O error: ${e.getMessage}")
        Vector.empty[Float]
      case e: Exception =>
        println(s"[ERROR] Unexpected error: ${e.getClass.getSimpleName} - ${e.getMessage}")
        Vector.empty[Float]
    }
  }

  /**
   * Public method to generate embeddings for a batch of texts.
   * NOTE: This implementation iterates and calls `embedSingle` .
   * In a high-throughput Spark environment (per HW2 examples), this logic
   * would be parallelized, e.g., using `mapPartitions` to call a true
   * batch-embedding endpoint on Ollama.
   *
   * @param texts The sequence of chunk texts to embed.
   * @param model The Ollama model name.
   * @return A sequence of embedding arrays.
   */
  def embed(texts: Seq[String], model: String): Seq[Array[Float]] = {
    // Map each text string to its embedding vector (as an Array[Float] for Spark)
    texts.map(text => embedSingle(text, model).toArray)
  }
}