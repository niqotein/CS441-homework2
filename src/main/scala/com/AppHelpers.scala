package com

import com.typesafe.config.ConfigFactory
import org.apache.pdfbox.pdmodel.PDDocument
import org.apache.pdfbox.text.PDFTextStripper
import java.text.Normalizer
import scala.util.Try // Make sure this import is present
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

// Helper object to load your application.conf file
object Config {
  private val config = ConfigFactory.load("application.conf")

  // Add any other configs you need
  val corpusPath = config.getString("app.corpusPath")
  val tablePath = config.getString("app.tablePath")

  // Chunking parameters
  val chunkSize = config.getInt("app.chunker.size")
  val chunkOverlap = config.getInt("app.chunker.overlap")

  val embeddingModel = config.getString("app.embedding.model")
  val embeddingVersion = config.getString("app.embedding.version")
  val ollamaHost = config.getString("app.embedding.ollamaHost")
}

// We'll put your PDF extraction logic in its own object
object PdfProcessor {
  /**
   * Extracts text from a PDF's byte content using Try for safety.
   * Returns text or an empty string if parsing fails.
   */
  def extractText(content: Array[Byte]): String = {
    Try {
      val document = PDDocument.load(content)
      val text = new PDFTextStripper().getText(document)
      document.close()
      text
    }.getOrElse("") // Returns "" on any failure during load/getText/close
  }
}

object TextNormalizer {
  /** Unifies Unicode, normalizes whitespace and case. */
  def normalize(text: String): String = {
    // Step 1: Unicode normalize (NFC)
    val normalized = Normalizer.normalize(text, Normalizer.Form.NFC)
    // Step 2: Replace all line breaks/tabs with space, and collapse multiple spaces
    normalized
      .replaceAll("[ \\t\\x0B\\f\\r\\n]+", " ")  // All whitespace (space, tabs, newlines) -> single space
      .trim                                       // Remove leading/trailing whitespace
      .toLowerCase                                 // Optional: lowercase if required by embedding model
  }
}

object Chunker {
  case class Chunk(chunkId: String, text: String, startOffset: Int, endOffset: Int, contentHash: String)

  import java.security.MessageDigest

  def sha256Hex(s: String): String = {
    MessageDigest.getInstance("SHA-256").digest(s.getBytes("UTF-8")).map("%02x".format(_)).mkString
  }
  /**
   * Generates chunks using a purely functional, tail-recursive approach.
   * This replaces the while-loop and mutable ArrayBuffer.
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
        val end = math.min(currentStart + chunkSize, text.length)
        val chunkText = text.substring(currentStart, end)
        val chunkId = sha256Hex(docId + "_" + currentStart + "_" + end)
        val contentHash = sha256Hex(chunkText)
        val newChunk = Chunk(chunkId, chunkText, currentStart, end, contentHash)
        // Calculate the next starting index using the fixed stride
        val nextStart = currentStart + (chunkSize - chunkOverlap)

        // Call the loop again with the new state, prepending the chunk to the accumulator
        loop(nextStart, newChunk :: acc)
      }
    }
    // Start the recursion and reverse the final list to get the correct chunk order.
    loop(0, Nil).reverse.toSeq
  }
}


// BEGIN OLLAMA CLIENT FOR EMBEDDING CHUNKS


final case class SingleEmbedRequest(model: String, prompt: String)
object SingleEmbedRequest {
  implicit val encoder: Encoder[SingleEmbedRequest] = deriveEncoder
}

final case class SingleEmbedResponse(embedding: Vector[Float])
object SingleEmbedResponse {
  implicit val decoder: Decoder[SingleEmbedResponse] = deriveDecoder
}

object OllamaClient {

  private val backend = OkHttpSyncBackend()
  private val embeddingUrl = s"${Config.ollamaHost}/api/embeddings"

  // Generates an embedding for a single piece of text.
  private def embedSingle(text: String, model: String): Vector[Float] = {
    val reqBody = SingleEmbedRequest(model, text)
    val request = basicRequest
      .post(uri"$embeddingUrl")
      .body(reqBody)
      .response(asJson[SingleEmbedResponse])
      .readTimeout(30.seconds)

    try {
      val response = request.send(backend)
      response.body match {
        case Right(embedResponse) =>
          embedResponse.embedding
        case Left(error) =>
          println(s"[ERROR] Ollama responded with failure for text='$text'")
          println(s"[DETAILS] HTTP code: ${response.code}, error: $error")
          Vector.empty[Float]
      }
    } catch {
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

  // Public method to generate embeddings for a batch of texts.
  def embed(texts: Seq[String], model: String): Seq[Array[Float]] = {
    texts.map(text => embedSingle(text, model).toArray)
  }
}


