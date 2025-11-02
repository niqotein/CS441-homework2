package com

// Core Spark SQL classes for DataFrame creation, transformations, and functions
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

// Logging library used for structured, leveled runtime logs (INFO, WARN, etc.)
import com.typesafe.scalalogging.LazyLogging

// Delta Lake table APIs used for atomic upserts and versioned writes
import io.delta.tables._

// Row, Schema, and DataFrame classes for defining and manipulating Spark data
import org.apache.spark.sql.{Row, DataFrame}
import org.apache.spark.sql.types._

// Typesafe Config for external configuration management (application.conf)
import com.typesafe.config.ConfigFactory

// Java utility to measure high-precision execution time in nanoseconds
import java.util.concurrent.TimeUnit

/**
 * ============================================================================
 * PROJECT: Homework 2 — Incremental Delta Indexer
 * ============================================================================
 * DESCRIPTION:
 *   This program implements a cloud-ready, incremental RAG (Retrieval-Augmented
 *   Generation) indexing pipeline using Spark + Delta Lake.  It automatically
 *   detects changes in a document corpus, re-embeds only updated text chunks,
 *   and publishes an atomic retrieval index snapshot.
 *
 *   Key Features:
 *     • Parallel Spark processing for scalability
 *     • Idempotent Delta MERGE upserts
 *     • Incremental “delta detection” and deletion handling
 *     • PDF text extraction, normalization, and chunking
 *     • On-demand embedding generation via Ollama
 *     • Atomic retrieval-index publication (blue/green rollout)
 *     • Rich runtime metrics (freshness %, dedup %, throughput, timings)
 * ============================================================================
 */
object Main extends LazyLogging {

  def main(args: Array[String]): Unit = {

    // ------------------------------------------------------------------------
    // INITIALIZATION AND CONFIGURATION
    // ------------------------------------------------------------------------

    // Start a high-precision timer to measure total pipeline execution time.
    val pipelineStartTime = System.nanoTime()

    // Configure PDFBox to handle CMYK color profiles entirely in Java
    System.setProperty("org.apache.pdfbox.rendering.UsePureJavaCMYKConversion", "true")

    // Load the application.conf file for user-defined paths and parameters.
    val config = ConfigFactory.load()

    // Build a SparkSession with Delta Lake extensions enabled.
    // Delta adds ACID transactions, versioning, and schema enforcement to Spark.
    val spark = SparkSession.builder
      .appName("Homework 2 Delta Indexer")
      .master("local[*]") // can be changed to "yarn" for AWS EMR deployment
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession initialized. Starting incremental indexing pipeline...")

    // ------------------------------------------------------------------------
    // STEP 1 — INGEST AND NORMALIZE PDF DOCUMENTS
    // ------------------------------------------------------------------------
    //  • Reads all PDF files from the configured corpus path
    //  • Extracts text using PdfProcessor (safe via Try)
    //  • Normalizes whitespace and case for consistency
    //  • Computes deterministic hashes for docId and contentHash
    // ------------------------------------------------------------------------

    val extractTextUDF = udf(PdfProcessor.extractText(_: Array[Byte]))
    val normalizeTextUDF = udf(TextNormalizer.normalize(_: String))

    val rawDocsDF = spark.read
      .format("binaryFile")
      .option("pathGlobFilter", "*.pdf")   // only load PDFs
      .load(Config.corpusPath)
      .select($"path", $"content")

    val normalizedDocsDF = rawDocsDF
      .withColumn("rawText", extractTextUDF($"content"))
      .withColumn("text", normalizeTextUDF($"rawText"))
      .select(
        $"path".as("uri"),
        $"text",
        sha2($"path", 256).as("docId"),          // stable ID based on file path
        sha2($"text", 256).as("contentHash")     // detects text modifications
      )
      .filter($"text" =!= "")                    // skip empty extractions

    logger.info("Successfully normalized all PDF documents.")
    val totalDocsInCorpus = normalizedDocsDF.count()

    val docTablePath = s"${Config.tablePath}/rag_doc_normalized"

    // Create the Delta table for normalized docs if it does not yet exist.
    if (!DeltaTable.isDeltaTable(spark, docTablePath)) {
      logger.warn(s"No existing table at $docTablePath — creating new one.")
      normalizedDocsDF.limit(0).write.format("delta").save(docTablePath)
    }

    // ------------------------------------------------------------------------
    // STEP 2 — DELTA DETECTION (NEW, CHANGED, AND DELETED DOCS)
    // ------------------------------------------------------------------------
    //  • Compares normalized text hashes with stored hashes
    //  • Identifies documents that are new or have changed content
    //  • Detects deletions by anti-joining the previous and current sets
    // ------------------------------------------------------------------------

    val existingDocsDF = spark.read.format("delta").load(docTablePath)

    val toProcessDF = normalizedDocsDF.join(
      existingDocsDF.select("docId", "contentHash"),
      Seq("docId", "contentHash"),
      "left_anti" // keeps only docs not identical to existing ones
    )

    toProcessDF.cache()
    val newOrChangedCount = toProcessDF.count()
    logger.info(s"Delta detection: $newOrChangedCount new or modified documents identified.")

    val deletedDocsDF = existingDocsDF.join(
      normalizedDocsDF.select("docId"), Seq("docId"), "left_anti"
    )
    val deletedCount = deletedDocsDF.count()
    logger.info(s"Deletion detection: $deletedCount documents removed from corpus.")

    // ------------------------------------------------------------------------
    // STEP 3 — DELETE ORPHANED RECORDS FROM ALL DELTA TABLES
    // ------------------------------------------------------------------------

    if (deletedCount > 0) {
      val chunkTablePath = s"${Config.tablePath}/rag_chunks"
      val embeddingsTablePath = s"${Config.tablePath}/rag_embeddings"
      val deletedDocIds = deletedDocsDF.select("docId").as[String].collect()

      // Cascade delete across dependent tables (docs, chunks, embeddings)
      DeltaTable.forPath(spark, docTablePath).delete(col("docId").isin(deletedDocIds: _*))
      if (DeltaTable.isDeltaTable(spark, chunkTablePath))
        DeltaTable.forPath(spark, chunkTablePath).delete(col("docId").isin(deletedDocIds: _*))
      if (DeltaTable.isDeltaTable(spark, embeddingsTablePath))
        DeltaTable.forPath(spark, embeddingsTablePath).delete(col("docId").isin(deletedDocIds: _*))

      logger.info("Deleted orphaned records related to removed documents.")
    }

    // ------------------------------------------------------------------------
    // STEP 4 — CHUNKING NEW/CHANGED DOCUMENTS
    // ------------------------------------------------------------------------
    //  • Splits text into overlapping fixed-size chunks
    //  • Each chunk receives a deterministic SHA-256 chunkId
    //  • Enables fine-grained embedding and delta comparison
    // ------------------------------------------------------------------------

    val chunkUDF = udf { (docId: String, text: String) =>
      Chunker.chunkDocument(docId, text, Config.chunkSize, Config.chunkOverlap)
        .map(c => (c.chunkId, c.text, c.startOffset, c.endOffset, c.contentHash))
    }

    val chunkedDF = toProcessDF
      .withColumn("chunks", explode(chunkUDF($"docId", $"text")))
      .select(
        $"docId",
        $"chunks._1".as("chunkId"),
        $"chunks._2".as("chunkText"),
        $"chunks._3".as("startOffset"),
        $"chunks._4".as("endOffset"),
        $"chunks._5".as("chunkContentHash")
      )

    logger.info("Chunking complete — sample output:")
    chunkedDF.show(5, truncate = 50)

    // Create or update the Delta table that stores chunk metadata.
    val chunkTablePath = s"${Config.tablePath}/rag_chunks"
    val chunkSchema = chunkedDF.schema
    if (!DeltaTable.isDeltaTable(spark, chunkTablePath)) {
      spark.createDataFrame(spark.sparkContext.emptyRDD[Row], chunkSchema)
        .write.format("delta").save(chunkTablePath)
      logger.info("Created new Delta table for chunk metadata.")
    }

    val chunkDeltaTable = DeltaTable.forPath(spark, chunkTablePath)
    if (!chunkedDF.isEmpty) {
      chunkDeltaTable.alias("target")
        .merge(chunkedDF.alias("source"), "target.chunkId = source.chunkId")
        .whenMatched().updateAll()
        .whenNotMatched().insertAll()
        .execute()
      logger.info("Chunk metadata upsert successful.")
    } else logger.info("No new chunks detected — skipping chunk upsert.")

    // ------------------------------------------------------------------------
    // STEP 5 — EMBEDDING GENERATION AND UPSERT
    // ------------------------------------------------------------------------
    //  • Selects only chunks missing embeddings for current model/version
    //  • Calls the Ollama API to generate embedding vectors
    //  • Upserts embeddings into Delta Lake for version tracking
    // ------------------------------------------------------------------------

    val currentEmbedder = Config.embeddingModel
    val currentVersion = Config.embeddingVersion
    val embeddingsTablePath = s"${Config.tablePath}/rag_embeddings"

    val embedSchema = StructType(Seq(
      StructField("chunkId", StringType, nullable = false),
      StructField("docId", StringType, nullable = false),
      StructField("embedding", ArrayType(FloatType), nullable = false),
      StructField("embedder", StringType, nullable = false),
      StructField("embedderVersion", StringType, nullable = false)
    ))

    if (!DeltaTable.isDeltaTable(spark, embeddingsTablePath)) {
      spark.createDataFrame(spark.sparkContext.emptyRDD[Row], embedSchema)
        .write.format("delta").save(embeddingsTablePath)
      logger.info("Created new Delta table for embeddings.")
    }

    val embeddingsDF = spark.read.format("delta").load(embeddingsTablePath)

    // Left-anti join finds only chunks that lack current embeddings
    val chunksToEmbed = chunkedDF.join(
      embeddingsDF
        .filter($"embedder" === currentEmbedder && $"embedderVersion" === currentVersion)
        .select("chunkId"),
      Seq("chunkId"),
      "left_anti"
    )

    val chunkIds = chunksToEmbed.select("chunkId").as[String].collect().toSeq
    val docIds = chunksToEmbed.select("docId").as[String].collect().toSeq
    val texts = chunksToEmbed.select("chunkText").as[String].collect().toSeq

    // Measure time spent on embedding generation
    val tEmbedStart = System.nanoTime()
    val embeddings: Seq[Array[Float]] = OllamaClient.embed(texts, currentEmbedder)
    val totalEmbedNanos = System.nanoTime() - tEmbedStart
    val chunksEmbeddedCount = embeddings.size
    logger.info(s"Embedding complete: $chunksEmbeddedCount new vectors generated.")

    // Build DataFrame of new embeddings for upsert
    val outRows = chunkIds.zip(docIds).zip(embeddings).map { case ((cid, did), vec) =>
      Row(cid, did, vec, currentEmbedder, currentVersion)
    }
    val newEmbeddingsDF = spark.createDataFrame(spark.sparkContext.parallelize(outRows), embedSchema)

    val embeddingsTable = DeltaTable.forPath(spark, embeddingsTablePath)
    if (!newEmbeddingsDF.isEmpty) {
      embeddingsTable.alias("target")
        .merge(
          newEmbeddingsDF.alias("source"),
          "target.chunkId = source.chunkId AND target.embedder = source.embedder AND target.embedderVersion = source.embedderVersion"
        )
        .whenMatched()
        .updateExpr(Map("embedding" -> "source.embedding", "docId" -> "source.docId"))
        .whenNotMatched()
        .insertExpr(Map(
          "chunkId" -> "source.chunkId",
          "docId" -> "source.docId",
          "embedding" -> "source.embedding",
          "embedder" -> "source.embedder",
          "embedderVersion" -> "source.embedderVersion"
        ))
        .execute()
      logger.info("Embedding table upsert successful.")
    } else logger.info("No new embeddings detected — skipping upsert.")

    // ------------------------------------------------------------------------
    // STEP 6 — UPSERT DOCUMENT METADATA TABLE
    // ------------------------------------------------------------------------
    // Ensures that any content-hash updates are reflected in the doc table.
    // ------------------------------------------------------------------------

    val deltaTable = DeltaTable.forPath(spark, docTablePath)
    deltaTable.alias("target")
      .merge(normalizedDocsDF.alias("source"), "target.docId = source.docId")
      .whenMatched("target.contentHash <> source.contentHash").updateAll()
      .whenNotMatched().insertAll().execute()
    logger.info("Document metadata table updated successfully.")

    toProcessDF.unpersist()

    // ------------------------------------------------------------------------
    // STEP 7 — RETRIEVAL INDEX MATERIALIZATION (CONDITIONAL)
    // ------------------------------------------------------------------------
    //  • Joins docs + chunks + embeddings into unified index
    //  • Writes an atomic Delta snapshot (“green” version)
    //  • Skips rebuild when no corpus or embedding changes occurred
    // ------------------------------------------------------------------------

    if (newOrChangedCount > 0 || deletedCount > 0 || !newEmbeddingsDF.isEmpty) {
      logger.info("Detected corpus or embedding changes — rebuilding retrieval index...")
      try {
        val retrievalIndexPath = s"${Config.tablePath}/rag_retrieval_index"
        if (
          DeltaTable.isDeltaTable(spark, docTablePath) &&
            DeltaTable.isDeltaTable(spark, chunkTablePath) &&
            DeltaTable.isDeltaTable(spark, embeddingsTablePath)
        ) {
          val docsDF = spark.read.format("delta").load(docTablePath)
          val chunksDF = spark.read.format("delta").load(chunkTablePath)
          val embedsDF = spark.read.format("delta").load(embeddingsTablePath)
            .where($"embedder" === currentEmbedder && $"embedderVersion" === currentVersion)

          val retrievalIndexDF = docsDF
            .select($"docId", $"uri", $"contentHash")
            .join(chunksDF, Seq("docId"))
            .join(
              embedsDF.select($"chunkId", $"embedding", $"embedder", $"embedderVersion"),
              Seq("chunkId")
            )
            .select(
              $"docId", $"uri", $"chunkId", $"chunkText",
              $"startOffset", $"endOffset",
              $"embedding", $"embedder", $"embedderVersion"
            )

          retrievalIndexDF.write
            .format("delta")
            .mode("overwrite")          // atomic publish
            .option("overwriteSchema", "true")
            .save(retrievalIndexPath)

          val finalCount = retrievalIndexDF.count()
          logger.info(s"Retrieval index rebuilt successfully with $finalCount rows.")
        } else logger.warn("Missing required Delta tables — skipping retrieval index build.")
      } catch {
        case e: Exception =>
          logger.warn(s"Retrieval index build failed: ${e.getMessage}")
          e.printStackTrace()
      }
    } else {
      logger.info("No data or embedding changes — retrieval index is current; skipping rebuild.")
    }

    // ------------------------------------------------------------------------
    // STEP 8 — LOG PIPELINE METRICS
    // ------------------------------------------------------------------------
    // Logs freshness, deduplication ratio, throughput, and timing information.
    // ------------------------------------------------------------------------

    val totalPipelineNanos = System.nanoTime() - pipelineStartTime
    logMetrics(
      totalDocsInCorpus,
      newOrChangedCount,
      deletedCount,
      chunksEmbeddedCount,
      totalEmbedNanos,
      totalPipelineNanos
    )

    logger.info("Incremental indexing pipeline completed successfully.")
    spark.stop()
  }

  // ==========================================================================
  // METRICS LOGGER — Computes and displays performance indicators
  // ==========================================================================
  private def logMetrics(
                          totalDocsInCorpus: Long,
                          newOrChangedCount: Long,
                          deletedCount: Long,
                          chunksEmbeddedCount: Int,
                          embedNanos: Long,
                          pipelineNanos: Long
                        ): Unit = {

    val pipelineMillis = TimeUnit.NANOSECONDS.toMillis(pipelineNanos)
    val embedMillis = TimeUnit.NANOSECONDS.toMillis(embedNanos)

    // Deduplication Ratio = Skipped Docs / Total Docs
    val docsSkipped = totalDocsInCorpus - newOrChangedCount
    val dedupRatio =
      if (totalDocsInCorpus > 0) (docsSkipped.toDouble / totalDocsInCorpus) * 100.0 else 0.0

    // Freshness % = New/Changed Docs / Total Docs
    val freshnessPct =
      if (totalDocsInCorpus > 0) (newOrChangedCount.toDouble / totalDocsInCorpus) * 100.0 else 0.0

    // Embedding Throughput = Chunks Embedded / Seconds
    val embedSeconds = embedNanos / 1_000_000_000.0
    val throughput = if (embedSeconds > 0.001) chunksEmbeddedCount / embedSeconds else 0.0

    logger.info("===================== PIPELINE METRICS REPORT =====================")
    logger.info(f"Total Pipeline Time:    $pipelineMillis ms")
    logger.info(f"Embedding Service Time: $embedMillis ms\n")
    logger.info("DOCUMENT METRICS:")
    logger.info(f"  Freshness:            $freshnessPct%.2f%%")
    logger.info(f"  Deduplication Ratio:  $docsSkipped / $totalDocsInCorpus skipped ($dedupRatio%.2f%%)")
    logger.info(f"  Documents Processed:  $newOrChangedCount new/changed")
    logger.info(f"  Documents Deleted:    $deletedCount\n")
    logger.info("EMBEDDING PERFORMANCE:")
    logger.info(f"  Chunks Embedded:      $chunksEmbeddedCount")
    logger.info(f"  Throughput:           $throughput%.2f chunks/sec")
    logger.info("===================================================================")
  }
}
