package com

// Core Spark SQL classes for DataFrame creation, transformations, and functions
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

// Logging library used for structured, leveled runtime logs (INFO, WARN, etc.)
import com.typesafe.scalalogging.LazyLogging

// Delta Lake table APIs used for atomic upserts (MERGE) and versioned writes
import io.delta.tables._

// Row, Schema, and DataFrame classes for defining and manipulating Spark data
import org.apache.spark.sql.{Row, DataFrame}
import org.apache.spark.sql.types._

// Typesafe Config for external configuration management (from AppHelpers)
import com.typesafe.config.ConfigFactory

// Java utility to measure high-precision execution time in nanoseconds
import java.util.concurrent.TimeUnit

/**
 * ============================================================================
 * PROJECT: Homework 2 — Incremental Delta Indexer
 * ============================================================================
 * DESCRIPTION:
 * This program implements a cloud-ready, incremental RAG (Retrieval-Augmented
 * Generation) indexing pipeline using Spark + Delta Lake. It automatically
 * detects changes in a document corpus (MSRCorpus), re-embeds only updated
 * text chunks using Ollama, and publishes an atomic retrieval index snapshot.
 *
 * Key Features:
 * • Parallel Spark processing for scalability (HW2 Main Textbook Group).
 * • Idempotent Delta MERGE upserts (for safe retries, HW2).
 * • Incremental “delta detection” (finds new/changed/deleted files, HW2).
 * • PDF text extraction, normalization, and chunking (from HW1).
 * • On-demand embedding generation via Ollama (from HW1).
 * • Atomic retrieval-index publication (for blue/green rollouts, HW2).
 * • Rich runtime metrics (freshness, dedup ratio, throughput, timings).
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
    // This is a common workaround for rendering errors in some PDFs.
    System.setProperty("org.apache.pdfbox.rendering.UsePureJavaCMYKConversion", "true")

    // Load the application.conf file (using the AppHelpers.Config object)
    val config = ConfigFactory.load()
    logger.info(s"Loaded config. Corpus: ${Config.corpusPath}, Chunker: ${Config.chunkSize}c / ${Config.chunkOverlap}o")

    // Build a SparkSession with Delta Lake extensions enabled.
    // This is the entry point for the HW2 Spark computational model.
    val spark = SparkSession.builder
      .appName("Homework 2 Delta Indexer")
      // `local[*]` runs locally using all available cores.
      // For AWS EMR deployment (per HW1/HW2), this would be removed, and
      // the app submitted with `spark-submit --master yarn ...`
      .master("local[*]")
      // These two configs enable Delta Lake features (ACID, MERGE, time travel)
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    // Import spark.implicits_ for helpful functions like `$` and `.toDF()`
    import spark.implicits._
    // Reduce log verbosity for a cleaner console output
    spark.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession initialized with Delta Lake. Starting pipeline...")

    // ------------------------------------------------------------------------
    // STEP 1 — INGEST AND NORMALIZE PDF DOCUMENTS
    // ------------------------------------------------------------------------
    //  • Reads all PDF files from the MSRCorpus path using `binaryFile` format.
    //  • Extracts text using the `PdfProcessor` helper.
    //  • Normalizes text using the `TextNormalizer` helper.
    //  • Computes deterministic hashes:
    //      - `docId` (from path): A stable ID for the document.
    //      - `contentHash` (from text): To detect if the content has changed.
    // ------------------------------------------------------------------------

    // Wrap the helper functions as Spark User-Defined Functions (UDFs)
    // This allows them to be called in parallel across the Spark cluster.
    val extractTextUDF = udf(PdfProcessor.extractText(_: Array[Byte]))
    val normalizeTextUDF = udf(TextNormalizer.normalize(_: String))

    // Read all PDF files. `binaryFile` format reads (path, modificationTime, length, content)
    val rawDocsDF = spark.read
      .format("binaryFile")
      .option("pathGlobFilter", "*.pdf")   // Ensure we only process PDFs
      .load(Config.corpusPath)
      .select($"path", $"content")         // We only need the path and raw byte content

    // Apply normalization and hashing transformations
    val normalizedDocsDF = rawDocsDF
      // Apply UDF to extract text from raw bytes
      .withColumn("rawText", extractTextUDF($"content"))
      // Apply UDF to clean the extracted text
      .withColumn("text", normalizeTextUDF($"rawText"))
      .select(
        $"path".as("uri"),
        $"text",
        // Create a stable `docId` using a hash of the file path. (HW2)
        sha2($"path", 256).as("docId"),
        // Create a `contentHash` using a hash of the *normalized text*. (HW2)
        // This is the key to detecting changed files.
        sha2($"text", 256).as("contentHash")
      )
      // Filter out any PDFs that failed to extract (e.g., corrupt, empty)
      .filter($"text" =!= "")

    logger.info("Step 1: Ingested and normalized all PDF documents.")
    // `count()` is a Spark action that triggers the processing defined above.
    val totalDocsInCorpus = normalizedDocsDF.count()

    // Define the path for our "documents" Delta table
    val docTablePath = s"${Config.tablePath}/rag_doc_normalized"

    // --- Schema Bootstrap ---
    // If the Delta table doesn't exist (first run), create it with the correct schema
    // This is necessary so that later `MERGE` operations have a target table.
    if (!DeltaTable.isDeltaTable(spark, docTablePath)) {
      logger.warn(s"No existing table at $docTablePath — creating new one.")
      // Write an empty DataFrame with the schema of `normalizedDocsDF`
      normalizedDocsDF.limit(0).write.format("delta").save(docTablePath)
    }

    // ------------------------------------------------------------------------
    // STEP 2 — DELTA DETECTION (NEW, CHANGED, AND DELETED DOCS)
    // ------------------------------------------------------------------------
    //  • This is the core logic of HW2: find *only* what changed.
    //  • `toProcessDF`: Finds new or modified docs using a `left_anti` join.
    //  • `deletedDocsDF`: Finds removed docs using a reverse `left_anti` join.
    // ------------------------------------------------------------------------

    // Load the existing, already-processed documents from our Delta table
    val existingDocsDF = spark.read.format("delta").load(docTablePath)

    // --- Find New/Changed Docs ---
    // `left_anti` join: Keep rows from the LEFT side (`normalizedDocsDF`)
    // that have NO match on the join keys (`docId`, `contentHash`) on the RIGHT side.
    // This efficiently finds:
    //   1. New docs (new `docId`).
    //   2. Changed docs (same `docId`, but new `contentHash`).
    val toProcessDF = normalizedDocsDF.join(
      existingDocsDF.select("docId", "contentHash"),
      Seq("docId", "contentHash"),
      "left_anti"
    )

    // Cache this DataFrame. It's small (only changed docs) and we use it
    // multiple times (for counting, chunking, and metrics).
    toProcessDF.cache()
    val newOrChangedCount = toProcessDF.count()
    logger.info(s"Step 2: Delta detection complete. Found $newOrChangedCount new or modified documents.")

    // --- Find Deleted Docs ---
    // Reverse join: Keep rows from `existingDocsDF` (our table) that
    // have NO match in `normalizedDocsDF` (the current corpus).
    val deletedDocsDF = existingDocsDF.join(
      normalizedDocsDF.select("docId"), Seq("docId"), "left_anti"
    )
    val deletedCount = deletedDocsDF.count()
    logger.info(s"Step 2: Deletion detection complete. Found $deletedCount removed documents.")

    // ------------------------------------------------------------------------
    // STEP 3 — DELETE ORPHANED RECORDS FROM ALL DELTA TABLES
    // ------------------------------------------------------------------------
    //  • If deletions were found, we must "cascade" the delete to all
    //    dependent tables (chunks, embeddings) to keep the index consistent.
    // ------------------------------------------------------------------------

    if (deletedCount > 0) {
      logger.warn(s"Deleting $deletedCount orphaned documents and their data...")
      val chunkTablePath = s"${Config.tablePath}/rag_chunks"
      val embeddingsTablePath = s"${Config.tablePath}/rag_embeddings"

      // Collect the list of `docId`s to delete.
      // This is safe as the list of deletions should be small.
      val deletedDocIds = deletedDocsDF.select("docId").as[String].collect()

      // Use the DeltaTable API to perform surgical `delete` operations
      DeltaTable.forPath(spark, docTablePath).delete(col("docId").isin(deletedDocIds: _*))
      // Also delete chunks from the deleted docs
      if (DeltaTable.isDeltaTable(spark, chunkTablePath))
        DeltaTable.forPath(spark, chunkTablePath).delete(col("docId").isin(deletedDocIds: _*))
      // And delete their embeddings
      if (DeltaTable.isDeltaTable(spark, embeddingsTablePath))
        DeltaTable.forPath(spark, embeddingsTablePath).delete(col("docId").isin(deletedDocIds: _*))

      logger.info("Step 3: Orphaned records deleted successfully.")
    }

    // ------------------------------------------------------------------------
    // STEP 4 — CHUNKING NEW/CHANGED DOCUMENTS
    // ------------------------------------------------------------------------
    //  • Applies the `Chunker.chunkDocument` UDF to the `toProcessDF`.
    //  • Uses `explode` to flatten the resulting arrays of chunks into rows.
    //  • Upserts the new/changed chunks into the `rag_chunks` Delta table.
    // ------------------------------------------------------------------------

    // Define a UDF that wraps the `chunkDocument` helper
    val chunkUDF = udf { (docId: String, text: String) =>
      Chunker.chunkDocument(docId, text, Config.chunkSize, Config.chunkOverlap)
        // Map the case class to a Tuple so Spark can understand it
        .map(c => (c.chunkId, c.text, c.startOffset, c.endOffset, c.contentHash))
    }

    // Apply the UDF to only the new/changed docs
    val chunkedDF = toProcessDF
      // This column will contain an ARRAY of chunk tuples
      .withColumn("chunks", explode(chunkUDF($"docId", $"text")))
      // `explode` "flattens" the array, creating a new row for each chunk
      .select(
        $"docId",
        $"chunks._1".as("chunkId"),
        $"chunks._2".as("chunkText"),
        $"chunks._3".as("startOffset"),
        $"chunks._4".as("endOffset"),
        $"chunks._5".as("chunkContentHash")
      )

    logger.info("Step 4: Chunking of new/changed documents complete.")
    if (config.getBoolean("app.debug")) chunkedDF.show(5, truncate = 50)

    // --- Upsert Chunks into Delta Lake ---
    val chunkTablePath = s"${Config.tablePath}/rag_chunks"
    val chunkSchema = chunkedDF.schema // Get schema for bootstrap

    // Bootstrap the `rag_chunks` table if it doesn't exist
    if (!DeltaTable.isDeltaTable(spark, chunkTablePath)) {
      spark.createDataFrame(spark.sparkContext.emptyRDD[Row], chunkSchema)
        .write.format("delta").save(chunkTablePath)
      logger.info("Created new Delta table for chunk metadata.")
    }

    val chunkDeltaTable = DeltaTable.forPath(spark, chunkTablePath)
    if (!chunkedDF.isEmpty) {
      // Perform an "upsert" (update or insert) using MERGE
      // This is the *idempotent* write operation required by HW2.
      // If the job fails and re-runs, this MERGE will safely
      // update existing chunks instead of creating duplicates.
      chunkDeltaTable.alias("target")
        .merge(chunkedDF.alias("source"), "target.chunkId = source.chunkId")
        .whenMatched().updateAll()     // If chunkId exists, update all fields
        .whenNotMatched().insertAll()  // If chunkId is new, insert it
        .execute()
      logger.info("Chunk metadata upserted into Delta table.")
    } else logger.info("No new chunks detected — skipping chunk upsert.")

    // ------------------------------------------------------------------------
    // STEP 5 — EMBEDDING GENERATION AND UPSERT
    // ------------------------------------------------------------------------
    //  • Identifies which of the new/changed chunks *actually* need embedding.
    //  • This is delta-aware: it skips chunks that already have an embedding
    //    for the *current* model and version.
    //  • Calls Ollama client, then upserts new embeddings into `rag_embeddings`.
    // ------------------------------------------------------------------------

    val currentEmbedder = Config.embeddingModel
    val currentVersion = Config.embeddingVersion
    val embeddingsTablePath = s"${Config.tablePath}/rag_embeddings"

    // Define the schema for the embeddings table
    val embedSchema = StructType(Seq(
      StructField("chunkId", StringType, nullable = false),
      StructField("docId", StringType, nullable = false),
      StructField("embedding", ArrayType(FloatType), nullable = false),
      StructField("embedder", StringType, nullable = false),
      StructField("embedderVersion", StringType, nullable = false)
    ))

    // Bootstrap the `rag_embeddings` table
    if (!DeltaTable.isDeltaTable(spark, embeddingsTablePath)) {
      spark.createDataFrame(spark.sparkContext.emptyRDD[Row], embedSchema)
        .write.format("delta").save(embeddingsTablePath)
      logger.info("Created new Delta table for embeddings.")
    }

    val embeddingsDF = spark.read.format("delta").load(embeddingsTablePath)

    // --- Find Chunks that *Need* Embedding ---
    // This join finds chunks from this run (`chunkedDF`) that
    // do NOT have a matching entry in the `embeddingsDF` for
    // the *current model and version*.
    val chunksToEmbed = chunkedDF.join(
      embeddingsDF
        .filter($"embedder" === currentEmbedder && $"embedderVersion" === currentVersion)
        .select("chunkId"), // We only need chunkId to check for existence
      Seq("chunkId"),
      "left_anti" // Keep only chunks from `chunkedDF` with no match
    )

    logger.info("Identifying chunks that need new embeddings...")

    // --- Collect, Embed, and Re-parallelize ---
    // NOTE: `collect()` pulls all data to the driver node.
    // This is simple, but for a *very* large job (billions of chunks),
    // this would be a bottleneck. The HW2 Spark pseudocode shows
    // a `mapPartitions` or `foreachBatch` approach where embedding
    // happens *in parallel* on the worker nodes.
    val chunkIds = chunksToEmbed.select("chunkId").as[String].collect().toSeq
    val docIds = chunksToEmbed.select("docId").as[String].collect().toSeq
    val texts = chunksToEmbed.select("chunkText").as[String].collect().toSeq

    logger.info(s"Step 5: Calling Ollama to embed ${texts.size} new chunks...")
    val tEmbedStart = System.nanoTime()
    // Call the helper client to get embeddings
    val embeddings: Seq[Array[Float]] = OllamaClient.embed(texts, currentEmbedder)
    val totalEmbedNanos = System.nanoTime() - tEmbedStart
    val chunksEmbeddedCount = embeddings.size
    logger.info(s"Embedding complete: $chunksEmbeddedCount vectors generated in ${totalEmbedNanos / 1e6} ms.")

    // --- Upsert New Embeddings ---
    // Zip the results back into Spark Rows
    val outRows = chunkIds.zip(docIds).zip(embeddings).map { case ((cid, did), vec) =>
      Row(cid, did, vec, currentEmbedder, currentVersion)
    }
    // Create a new DataFrame from the results
    val newEmbeddingsDF = spark.createDataFrame(spark.sparkContext.parallelize(outRows), embedSchema)

    val embeddingsTable = DeltaTable.forPath(spark, embeddingsTablePath)
    if (!newEmbeddingsDF.isEmpty) {
      // MERGE the new embeddings into the table
      // The key includes the version, allowing multiple versions of
      // embeddings for the same chunk to co-exist.
      embeddingsTable.alias("target")
        .merge(
          newEmbeddingsDF.alias("source"),
          "target.chunkId = source.chunkId AND target.embedder = source.embedder AND target.embedderVersion = source.embedderVersion"
        )
        .whenMatched() // If for some reason we re-run, update the vector
        .updateExpr(Map("embedding" -> "source.embedding", "docId" -> "source.docId"))
        .whenNotMatched() // This is the normal path
        .insertExpr(Map(
          "chunkId" -> "source.chunkId",
          "docId" -> "source.docId",
          "embedding" -> "source.embedding",
          "embedder" -> "source.embedder",
          "embedderVersion" -> "source.embedderVersion"
        ))
        .execute()
      logger.info("Embedding table upsert successful.")
    } else logger.info("No new embeddings were generated — skipping upsert.")

    // ------------------------------------------------------------------------
    // STEP 6 — UPSERT DOCUMENT METADATA TABLE
    // ------------------------------------------------------------------------
    //  • Now that all chunks/embeddings are processed, we commit the
    //    changes to the main document table (`rag_doc_normalized`).
    //  • This MERGE updates `contentHash` for changed docs and inserts new docs.
    // ------------------------------------------------------------------------

    val deltaTable = DeltaTable.forPath(spark, docTablePath)
    deltaTable.alias("target")
      // Merge against *all* docs found in the corpus this run
      .merge(normalizedDocsDF.alias("source"), "target.docId = source.docId")
      // If doc exists but hash differs, update it
      .whenMatched("target.contentHash <> source.contentHash").updateAll()
      // If docId is new, insert it
      .whenNotMatched().insertAll().execute()
    logger.info("Step 6: Document metadata table updated successfully.")

    // Release the cached DataFrame
    toProcessDF.unpersist()

    // ------------------------------------------------------------------------
    // STEP 7 — RETRIEVAL INDEX MATERIALIZATION (CONDITIONAL)
    // ------------------------------------------------------------------------
    //  • This is the final step: create the "denormalized" index for serving.
    //  • It JOINS docs, chunks, and embeddings into one flat, fast table.
    //  • This step is *skipped* if no data changed, saving compute.
    //  • It uses `mode("overwrite")` for an *atomic publish* (HW2).
    // ------------------------------------------------------------------------

    // Only rebuild the final index if something *actually changed*
    if (newOrChangedCount > 0 || deletedCount > 0 || !newEmbeddingsDF.isEmpty) {
      logger.info("Step 7: Detected changes. Rebuilding final retrieval index...")
      try {
        val retrievalIndexPath = s"${Config.tablePath}/rag_retrieval_index"

        // Ensure all source tables exist before trying to join
        if (
          DeltaTable.isDeltaTable(spark, docTablePath) &&
            DeltaTable.isDeltaTable(spark, chunkTablePath) &&
            DeltaTable.isDeltaTable(spark, embeddingsTablePath)
        ) {
          // Read the full, updated source tables
          val docsDF = spark.read.format("delta").load(docTablePath)
          val chunksDF = spark.read.format("delta").load(chunkTablePath)
          val embedsDF = spark.read.format("delta").load(embeddingsTablePath)
            // CRITICAL: Filter to *only* the model/version we want in this index
            .where($"embedder" === currentEmbedder && $"embedderVersion" === currentVersion)

          // Join the three "normalized" tables into one "denormalized" table
          val retrievalIndexDF = docsDF
            .select($"docId", $"uri", $"contentHash")
            .join(chunksDF, Seq("docId")) // Join docs + chunks
            .join(
              embedsDF.select($"chunkId", $"embedding", $"embedder", $"embedderVersion"),
              Seq("chunkId") // Join result + embeddings
            )
            .select( // Select the final fields for the index
              $"docId", $"uri", $"chunkId", $"chunkText",
              $"startOffset", $"endOffset",
              $"embedding", $"embedder", $"embedderVersion"
            )

          // --- Atomic Publish ---
          // `mode("overwrite")` on a Delta table is an ATOMIC operation.
          // Downstream query engines will see the *old* version of the index
          // until this write fully succeeds, then they switch to the new one.
          // This enables the "blue/green" deployment mentioned in HW2.
          retrievalIndexDF.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true") // Allows schema to evolve if needed
            .save(retrievalIndexPath)

          val finalCount = retrievalIndexDF.count()
          logger.info(s"Retrieval index rebuilt successfully with $finalCount rows.")
        } else logger.warn("Missing required Delta tables — skipping retrieval index build.")
      } catch {
        // Log errors but don't crash the whole pipeline
        case e: Exception =>
          logger.warn(s"Retrieval index build failed: ${e.getMessage}")
          e.printStackTrace()
      }
    } else {
      // This is the "no-op" run described in HW2
      logger.info("Step 7: No data or embedding changes detected. Retrieval index is current; skipping rebuild.")
    }

    // ------------------------------------------------------------------------
    // STEP 8 — LOG PIPELINE METRICS
    // ------------------------------------------------------------------------
    //  • Logs freshness, deduplication ratio, throughput, and timing.
    //  • This is key for evaluating the *cost* and *freshness* of the
    //    incremental pipeline, a core goal of HW2.
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
  /**
   * Logs a final report of the pipeline's performance.
   * This helps quantify the "freshness" and "cost" (deduplication)
   * benefits of the incremental approach (HW2).
   */
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

    // Deduplication Ratio = (Skipped Docs / Total Docs)
    // Measures how much work was *saved*. A 99% ratio means we skipped 99% of docs.
    val docsSkipped = totalDocsInCorpus - newOrChangedCount
    val dedupRatio =
      if (totalDocsInCorpus > 0) (docsSkipped.toDouble / totalDocsInCorpus) * 100.0 else 0.0

    // Freshness % = (New or Changed Docs / Total Docs)
    // Measures what percentage of the corpus was processed.
    val freshnessPct =
      if (totalDocsInCorpus > 0) (newOrChangedCount.toDouble / totalDocsInCorpus) * 100.0 else 0.0

    // Embedding Throughput = (Chunks Embedded / Seconds)
    // Measures the performance of the embedding service (Ollama).
    val embedSeconds = embedNanos / 1_000_000_000.0
    val throughput = if (embedSeconds > 0.001) chunksEmbeddedCount / embedSeconds else 0.0

    logger.info("===================== PIPELINE METRICS REPORT =====================")
    logger.info(f"Total Pipeline Time:    $pipelineMillis ms")
    logger.info(f"Embedding Service Time: $embedMillis ms\n")
    logger.info("DOCUMENT METRICS:")
    logger.info(f"  Total Docs in Corpus: $totalDocsInCorpus")
    logger.info(f"  Freshness (Processed):$newOrChangedCount docs ($freshnessPct%.2f%%)")
    logger.info(f"  Deduplication (Skipped):$docsSkipped docs ($dedupRatio%.2f%%)")
    logger.info(f"  Documents Deleted:    $deletedCount\n")
    logger.info("EMBEDDING PERFORMANCE:")
    logger.info(f"  New Chunks Embedded:  $chunksEmbeddedCount")
    logger.info(f"  Ollama Throughput:    $throughput%.2f chunks/sec")
    logger.info("===================================================================")
  }
}