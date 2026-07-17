# Incremental Delta RAG Indexer (Spark + Delta Lake + Ollama)

* **Author**: `Pranay Dhopate`
* **Email**: `pdhop@uic.edu`
* **YT Video**: [Video Link](https://youtu.be/7Pq5vegbT2g)
## Project Overview

This project is the second stage in a three-homework pipeline to build a complete Retrieval-Augmented Generation (RAG) system.  
Homework 1 created a **batch MapReduce RAG indexer**.  
Homework 2 upgrades the pipeline into a **continuous incremental indexing system** using:

## Tech Stack & Versions

| Component | Version | Notes |
|---|---|---|
| **Scala** | 2.13.x | Language for Spark + Delta application |
| **SBT** | 1.x | Build & dependency management |
| **Java (OpenJDK)** | 11 | Required by Spark/Delta |
| **Apache Spark** | 3.4.x | Distributed processing engine |
| **Delta Lake** | 2.4.0 | ACID storage & MERGE operations |
| **Ollama** | Latest (CPU) | Local embedding server |
| **Embedding Model** | `mxbai-embed-large` v1.3.0 | Pulled via `ollama pull mxbai-embed-large` |
| **PDF Processing** | Apache PDFBox 2.0.x | Text extraction from PDFs |
| **Config Loader** | Typesafe Config | Externalized config management |
| **Logging** | Scala-logging | Structured logging for pipeline steps |
| **Testing** | ScalaTest 3.2.x | Unit & integration tests |



The goal is to efficiently maintain a document → chunk → embedding index as PDFs are added, updated, or removed — without reprocessing unchanged data.

Unlike Homework 1 which rebuilt the index from scratch, this homework implements:

- **Change detection** (new vs modified PDFs)
- **Deterministic chunking**
- **Embedding reuse** (avoid duplicate work)
- **Delta MERGE upserts**
- **Cascade deletion**
- **Atomic retrieval index publishing**
- **Idempotency** (no-change runs do zero work)

---

## Program Flow & Design

The system is built as a **local + cloud-ready Spark application** with three Delta tables:

| Table | Contents |
|---|---|
`rag_doc_normalized` | Extracted & cleaned text + document hash |
`rag_chunks` | Deterministic text chunks + hash + offsets |
`rag_embeddings` | Chunk vectors + model version |
`rag_retrieval_index` | Final ready-to-query RAG table |

### Incremental Processing Logic

1. **Scan corpus and extract PDFs**
2. **Normalize text**
3. **Hash content**
4. **Compare with Delta table**
    - new docs → index
    - modified docs → re-index
    - deleted docs → cascade delete
5. **Chunk text**
6. **Embed only new chunks**
7. **MERGE into Delta tables**
8. **Rebuild retrieval index only if changes occurred**

### Idempotency Guarantee

Running the pipeline again with no changes yields:

- ✅ `0 new docs`
- ✅ `0 changed chunks`
- ✅ `0 embeddings computed`
- ✅ “skip rebuild” message

---

## Execution Phases

### 📦 Phase 1 — Local Development & Execution

This system is developed & tested locally using **Manjaro (Arch-based)** Linux.

#### 1) Install Dependencies

**JDK 11**
```bash
sudo pacman -S jdk11-openjdk
```
**SBT**
```bash
sudo pacman -S sbt
```
**Scala (Coursier)**
```bash
curl -fL "https://github.com/coursier/launchers/raw/master/cs-x86_64-pc-linux.gz" | gzip -d > cs
chmod +x cs
./cs setup
``` 
**Ollama**
```bash
paru -S ollama
sudo systemctl enable ollama
sudo systemctl start ollama
ollama pull mxbai-embed-large
``` 

#### 2) Download Corpus
Place `MSRCorpus`/ (PDFs) at repo root.

#### 3) Configure Paths
Edit `src/main/resources/application.conf`:
```bash
corpusPath = "/absolute/path/to/MSRCorpus"
tablePath  = "./target/delta-tables"
```
#### 4) Build & Test
```bash
sbt clean compile test
```
#### 5) Run Pipeline
```bash
sbt clean compile run
```
Expected behavior:

| Run | Behavior |
|---|---|
1st run | Full index build |
2nd run | Zero-work incremental pass |

Logs include metrics for:

- Freshness %
- Skip ratios
- Chunk count
- Embedding count
- Time breakdowns

---

### ☁️ Phase 2 — AWS EMR Deployment

EMR cluster automatically scales Spark workloads.  
Outputs are stored in **S3 Delta tables**.

#### S3 Layout

| Path | Purpose |
|---|---|
`s3://bucket/MSRCorpus/` | PDF Corpus |
`s3://bucket/delta-tables/` | Delta tables |
`s3://bucket/jars/` | Fat JAR |
`s3://bucket/conf/` | Config overrides |

#### Build Fat JAR

```bash
sbt clean assembly
aws s3 cp target/scala-2.13/*assembly*.jar s3://bucket/jars/
```

#### EMR Config Example

```bash
corpusPath = "s3://bucket/MSRCorpus"
tablePath  = "s3://bucket/delta-tables"
embedding.ollamaHost = "http://localhost:11434"
```

#### Submit Job

```bash
spark-submit \
  --class com.Main \
  --deploy-mode client \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --packages io.delta:delta-core_2.13:2.4.0 \
  s3://bucket/jars/441homework2-assembly.jar
```
## Verification & Metrics

### Key Success Criteria

| Requirement | Verified |
|---|---|
No-op incremental run | ✅  
Delta Lake MERGE | ✅  
Cascade deletion | ✅  
Atomic publish | ✅  
Normalized PDFs | ✅  
Chunking + overlap | ✅  
Ollama embeddings | ✅

### Test Suite

`Homework2Suite.scala` validates:

- Spark session init
- Text normalization
- Chunk overlap logic
- Delta MERGE semantics
- Empty Delta tables report zero rows (not a full end-to-end no-op pipeline check — see "Known Limitations" below)

---

## Known Limitations vs. Assignment Brief

This section documents places where the current implementation intentionally or unintentionally falls short of the original assignment brief. These are recorded here rather than silently left undocumented.

* **Embedding is sequential and driver-side, not batched across Spark workers.** The brief's own Spark pseudocode embeds via `mapPartitions { ... .grouped(64) ... }` with `foreachBatch` streaming, so embedding calls run in parallel on worker nodes. The actual pipeline instead `.collect()`s all chunk texts needing embeddings to the driver and calls `OllamaClient.embed` one blocking HTTP request at a time in a loop. Functionally correct at this sample corpus's scale, but won't scale to a large corpus — a scalability caveat, not a correctness bug.
* **The "no-op" test doesn't exercise the real pipeline end-to-end.** `Homework2Suite.scala`'s Test 5 only creates two empty Delta tables and checks their row counts — it never calls `Main`'s actual pipeline logic, and doesn't assert `OllamaClient.embed` isn't invoked on a true no-op rerun. The idempotency guarantee itself *was* verified manually by running `sbt "runMain com.Main"` twice against the sample corpus (second run: 0 new docs, 0 chunks embedded, 100% dedup ratio) — just not by this automated test yet. A stronger test would mock/count `OllamaClient.embed` calls across two real pipeline runs.

## Conclusion

Homework-2 builds a **production-style incremental RAG indexing engine** with:

- Spark compute engine
- Delta Lake ACID storage
- Efficient incremental ingestion + embedding
- Deterministic chunking & version control
- Local + Cloud (EMR) execution
- Fully idempotent behavior (zero-work no-op runs)
- Automatic deletion handling + atomic index publication

This system now handles real-world continuous document updates efficiently, and serves as the foundation for Homework-3: **RAG querying + LLM retrieval pipelines**.

---

## How to Build & Run (Summary)

| Step | Command |
|---|---|
Run tests | `sbt clean compile test` |
Run local pipeline | `sbt clean compile run` |
Build assembly JAR | `sbt clean assembly` |
Run on EMR | `spark-submit …` |

---


