package com

import io.circe.syntax._
import io.circe.parser.decode
import org.scalatest.funsuite.AnyFunSuite

/**
 * Pipeline stage: OLLAMA REQUEST/RESPONSE JSON (`SingleEmbedRequest`/`SingleEmbedResponse`).
 *
 * `OllamaClient` builds requests and parses responses purely through these two case
 * classes' Circe codecs; a field-name or shape mismatch here would silently break every
 * embedding call rather than fail loudly, so the codecs are tested directly against the
 * exact JSON shape Ollama's `/api/embeddings` endpoint actually uses.
 */
class EmbeddingJsonSpec extends AnyFunSuite {

  test("SingleEmbedRequest encodes with the exact field names Ollama's API expects") {
    val request = SingleEmbedRequest("mxbai-embed-large", "some chunk text")
    val json = request.asJson
    val cursor = json.hcursor

    assert(cursor.get[String]("model").toOption.contains("mxbai-embed-large"))
    assert(cursor.get[String]("prompt").toOption.contains("some chunk text"))
  }

  test("SingleEmbedResponse decodes Ollama's embedding array response") {
    val responseJson = """{"embedding": [0.1, 0.2, 0.3]}"""
    val decoded = decode[SingleEmbedResponse](responseJson)

    assert(decoded.isRight)
    assert(decoded.toOption.get.embedding == Vector(0.1f, 0.2f, 0.3f))
  }

  test("SingleEmbedResponse decoding fails clearly (Left) on a malformed response, rather than crashing") {
    val malformed = """{"unexpected_field": "no embedding here"}"""
    assert(decode[SingleEmbedResponse](malformed).isLeft)
  }
}
