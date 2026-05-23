# spark-asn1

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

A schema-driven Apache Spark data source for reading ASN.1-encoded files — BER, DER, Aligned PER, Unaligned PER, and XER — without any code-generation step.

Supply a `.asn1` schema file and a root type name; spark-asn1 parses the schema at runtime and decodes binary records directly into Spark SQL rows.

## Features

- **All major encodings**: BER, DER, Aligned PER, Unaligned PER, XER (Basic and Canonical)
- **No code generation**: schema is parsed at runtime from a plain `.asn1` file
- **Multi-module schemas**: `IMPORTS` across multiple schema files are resolved automatically
- **Full type coverage**: SEQUENCE, SET, SEQUENCE OF, SET OF, CHOICE, BIT STRING (named and unnamed), ENUMERATED, OCTET STRING, all string types, OID, INTEGER, BOOLEAN, NULL
- **Native Spark integration**: registered as a `FileFormat`; works with `spark.read`, schema inference, and partition discovery

## Requirements

- Apache Spark 3.5.x
- Scala 2.13 or 2.12
- Java 11+

## Installation

**SBT**
```scala
libraryDependencies += "io.github.nidhal-saadaoui" %% "spark-asn1" % "0.1.3"
```

**Maven**
```xml
<dependency>
  <groupId>io.github.nidhal-saadaoui</groupId>
  <artifactId>spark-asn1_2.13</artifactId>
  <version>0.1.3</version>
</dependency>
```

**Gradle**
```groovy
implementation 'io.github.nidhal-saadaoui:spark-asn1_2.13:0.1.3'
```

> spark-asn1 shades BouncyCastle and ANTLR4 internally, so there are no classpath conflicts on Spark clusters.

## Quick start

```scala
val df = spark.read
  .format("asn1")
  .option("asn1.schema", "/path/to/MySchema.asn1")
  .option("asn1.type",   "MyMessage")
  .option("asn1.encoding", "ber")        // ber | der | per-aligned | per-unaligned | xer
  .load("/data/messages/*.ber")

df.printSchema()
df.show()
```

## Options

| Option | Required | Default | Description |
|---|---|---|---|
| `asn1.schema` | yes | — | Comma-separated paths to `.asn1` schema files |
| `asn1.type` | yes | — | Name of the root ASN.1 type to decode |
| `asn1.encoding` | no | `ber` | `ber`, `der`, `per-aligned`, `per-unaligned`, `xer` |
| `asn1.per.framing` | no | `length-prefixed` | PER record framing: `length-prefixed`, `fixed-length`, `hex-lines` |
| `asn1.per.record.bytes` | no* | — | Record size in bytes; required when framing is `fixed-length` |
| `asn1.choice.tag.field` | no | `_tag` | Name of the CHOICE discriminator field |
| `asn1.enumerated.as.int` | no | `false` | Return ENUMERATED values as integers instead of symbolic names |

## Type mapping

| ASN.1 type | Spark SQL type | Notes |
|---|---|---|
| BOOLEAN | BooleanType | |
| NULL | NullType | |
| INTEGER | LongType | Always Long; cast down if needed |
| OCTET STRING | BinaryType | |
| ANY | BinaryType | Raw DER bytes |
| BIT STRING (unnamed) | BinaryType | Padding bits dropped |
| BIT STRING (named) | StructType(`_bytes` Binary, `_namedBits` Array[String]) | `_namedBits` lists the names of the set bits |
| All string types | StringType | UTF8String, PrintableString, IA5String, etc. |
| OBJECT IDENTIFIER | StringType | Dot-notation, e.g. `1.2.840.10045.2.1` |
| ENUMERATED | StringType | Symbolic name; set `asn1.enumerated.as.int=true` for integer |
| SEQUENCE / SET | StructType | OPTIONAL and DEFAULT fields are nullable |
| SEQUENCE OF / SET OF | ArrayType | |
| CHOICE | StructType(`_tag` String, alt1?, alt2?, …) | `_tag` holds the active alternative name; only that field is non-null |

## Examples

### BER — a simple SEQUENCE

Schema (`person.asn1`):
```asn1
PersonSchema DEFINITIONS AUTOMATIC TAGS ::= BEGIN
  Person ::= SEQUENCE {
    id     INTEGER,
    name   UTF8String,
    active BOOLEAN
  }
END
```

```scala
val df = spark.read
  .format("asn1")
  .option("asn1.schema",   "person.asn1")
  .option("asn1.type",     "Person")
  .option("asn1.encoding", "ber")
  .load("people.ber")

// +---+-----+------+
// | id| name|active|
// +---+-----+------+
// |  1|Alice|  true|
// |  2|  Bob| false|
// +---+-----+------+
```

### XER — multiple records in one file

```scala
val df = spark.read
  .format("asn1")
  .option("asn1.schema",   "person.asn1")
  .option("asn1.type",     "Person")
  .option("asn1.encoding", "xer")
  .load("people.xer")     // bare records or wrapped in <Records>…</Records>
```

### PER — fixed-length framing

```scala
val df = spark.read
  .format("asn1")
  .option("asn1.schema",           "message.asn1")
  .option("asn1.type",             "Message")
  .option("asn1.encoding",         "per-aligned")
  .option("asn1.per.framing",      "fixed-length")
  .option("asn1.per.record.bytes", "128")
  .load("messages.per")
```

### CHOICE — reading the discriminator

A CHOICE type maps to a struct with a string discriminator field (`_tag` by default) plus one nullable field per alternative. Only the field matching `_tag` is non-null.

Schema:
```asn1
Shape ::= CHOICE {
  circle    Circle,
  rectangle Rectangle
}
```

```scala
val df = spark.read
  .format("asn1")
  .option("asn1.schema",   "shapes.asn1")
  .option("asn1.type",     "Shape")
  .option("asn1.encoding", "ber")
  .load("shapes.ber")

// Resulting schema: _tag STRING NOT NULL, circle STRUCT<…>, rectangle STRUCT<…>
df.filter($"_tag" === "circle").select($"circle.*").show()

// Pattern-match with CASE WHEN:
import org.apache.spark.sql.functions._
df.select(
  when($"_tag" === "circle",    $"circle.radius")
   .when($"_tag" === "rectangle", $"rectangle.width")
   .as("dimension")
).show()
```

**CHOICE nested inside a SEQUENCE** — the most common case. Access alternatives via the parent field:

```asn1
Message ::= SEQUENCE {
  id      INTEGER,
  payload CHOICE {
    text  UTF8String,
    bytes OCTET STRING
  }
}
```

```scala
val df = spark.read.format("asn1")
  .option("asn1.schema", "msg.asn1").option("asn1.type", "Message")
  .option("asn1.encoding", "ber").load("messages.ber")

// payload maps to STRUCT<_tag STRING, text STRING, bytes BINARY>
df.select($"id", $"payload._tag", $"payload.text")
  .filter($"payload._tag" === "text")
  .show()
```

**Renaming the discriminator field** — useful when `_tag` collides with an existing field name:

```scala
spark.read.format("asn1")
  .option("asn1.schema",           "shapes.asn1")
  .option("asn1.type",             "Shape")
  .option("asn1.choice.tag.field", "kind")   // discriminator is now "kind"
  .load("shapes.ber")
```

### Multi-module schemas

```scala
spark.read
  .format("asn1")
  .option("asn1.schema",   "common.asn1,protocol.asn1,extensions.asn1")
  .option("asn1.type",     "ProtocolMessage")
  .option("asn1.encoding", "der")
  .load("data/")
```

## PER framing modes

PER streams do not self-delimit records, so a framing strategy is required:

| Mode | Description |
|---|---|
| `length-prefixed` | 4-byte big-endian length prefix before each record (default) |
| `fixed-length` | Every record is exactly `asn1.per.record.bytes` bytes |
| `hex-lines` | Each non-blank line is a hex-encoded record (useful for testing) |

## Building from source

```bash
git clone https://github.com/nidhal-saadaoui/spark-asn1.git
cd spark-asn1
sbt test          # run all 156 tests
sbt assembly      # build a shaded fat JAR
```

Cross-build for both Scala versions:
```bash
sbt +compile
sbt +test
```

## PySpark — standalone spark-submit

No cluster needed. Download the assembly JAR from the [GitHub Releases](https://github.com/nidhal-saadaoui/spark-asn1/releases) page and pass it with `--jars`:

```python
# read_ber.py
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("asn1").getOrCreate()

df = (spark.read
    .format("asn1")
    .option("asn1.schema",   "person.asn1")
    .option("asn1.type",     "Person")
    .option("asn1.encoding", "ber")
    .load("people.ber"))

df.printSchema()
df.show()
spark.stop()
```

```bash
spark-submit \
  --jars spark-asn1_2.13-0.1.3-assembly.jar \
  read_ber.py
```

For a Databricks cluster or EMR, upload the JAR to DBFS/S3 and attach it as a cluster library — no `--jars` flag needed.

## Containerized examples

### docker-compose — local Spark cluster

Build the fat JAR first, then start a three-node Spark cluster (one master, two workers) and submit a job that reads a BER file.

**Step 1 — build the assembly JAR**

```bash
sbt assembly
# produces target/scala-2.13/spark-asn1_2.13-0.1.3-assembly.jar
```

**Step 2 — create the project layout**

```
containerized/
├── docker-compose.yml
├── data/
│   └── people.ber          # your BER-encoded file
├── schemas/
│   └── person.asn1         # your ASN.1 schema
└── jobs/
    └── read_ber.py          # PySpark job (see below)
```

**`containerized/docker-compose.yml`**

```yaml
version: "3.8"

x-spark-common: &spark-common
  image: bitnami/spark:3.5
  environment:
    - SPARK_MODE=worker
    - SPARK_MASTER_URL=spark://spark-master:7077
    - SPARK_WORKER_MEMORY=2G
    - SPARK_WORKER_CORES=2

services:
  spark-master:
    <<: *spark-common
    environment:
      - SPARK_MODE=master
    ports:
      - "8080:8080"   # Spark Web UI
      - "7077:7077"   # Spark master port
    volumes:
      - ../target/scala-2.13:/opt/jars
      - ./data:/opt/data
      - ./schemas:/opt/schemas
      - ./jobs:/opt/jobs

  spark-worker-1:
    <<: *spark-common
    volumes:
      - ../target/scala-2.13:/opt/jars
      - ./data:/opt/data
      - ./schemas:/opt/schemas

  spark-worker-2:
    <<: *spark-common
    volumes:
      - ../target/scala-2.13:/opt/jars
      - ./data:/opt/data
      - ./schemas:/opt/schemas
```

**`containerized/jobs/read_ber.py`**

```python
from pyspark.sql import SparkSession

spark = (SparkSession.builder
    .appName("spark-asn1-demo")
    .getOrCreate())

df = (spark.read
    .format("asn1")
    .option("asn1.schema",   "/opt/schemas/person.asn1")
    .option("asn1.type",     "Person")
    .option("asn1.encoding", "ber")
    .load("/opt/data/people.ber"))

df.printSchema()
df.show()

spark.stop()
```

**Step 3 — start the cluster**

```bash
cd containerized
docker-compose up -d
# Wait for the master UI at http://localhost:8080
```

**Step 4 — submit the job**

```bash
docker-compose exec spark-master \
  spark-submit \
    --master spark://spark-master:7077 \
    --jars /opt/jars/spark-asn1_2.13-0.1.3-assembly.jar \
    /opt/jobs/read_ber.py
```

For a Scala/Java job, pass `--class` instead:

```bash
docker-compose exec spark-master \
  spark-submit \
    --master spark://spark-master:7077 \
    --class com.example.MyJob \
    --jars /opt/jars/spark-asn1_2.13-0.1.3-assembly.jar \
    /opt/jars/my-job.jar
```

---

### Jupyter notebook (PySpark)

Add a notebook service to `docker-compose.yml` for interactive exploration:

```yaml
  notebook:
    image: jupyter/pyspark-notebook:spark-3.5.0
    ports:
      - "8888:8888"
    environment:
      - PYSPARK_SUBMIT_ARGS=--master spark://spark-master:7077
          --jars /opt/jars/spark-asn1_2.13-0.1.3-assembly.jar pyspark-shell
    volumes:
      - ../target/scala-2.13:/opt/jars
      - ./data:/opt/data
      - ./schemas:/opt/schemas
      - ./notebooks:/home/jovyan/work
```

Then open `http://localhost:8888` and create a notebook:

```python
from pyspark.sql import SparkSession

spark = (SparkSession.builder
    .appName("asn1-notebook")
    .getOrCreate())

df = (spark.read
    .format("asn1")
    .option("asn1.schema",   "/opt/schemas/person.asn1")
    .option("asn1.type",     "Person")
    .option("asn1.encoding", "ber")
    .load("/opt/data/people.ber"))

df.show()
```

---

### Indexing large BER files inside the cluster

Run the indexer as a Spark job before your main ETL:

```python
# index_ber.py — run once to make reads parallel
from pyspark.sql import SparkSession

spark = (SparkSession.builder
    .appName("asn1-index")
    .getOrCreate())

# Dynamically import via Py4J gateway
jvm   = spark._jvm
Path  = jvm.org.apache.hadoop.fs.Path
indexer = jvm.io.github.sparkasn1.spark.asn1.util.Asn1Indexer

results = indexer.buildIndexes(
    spark._jsparkSession,
    "/opt/data/*.ber",
    False   # overwrite=False: skip already-indexed files
)
results.show()

spark.stop()
```

Submit it the same way:

```bash
docker-compose exec spark-master \
  spark-submit \
    --master spark://spark-master:7077 \
    --jars /opt/jars/spark-asn1_2.13-0.1.3-assembly.jar \
    /opt/jobs/index_ber.py
```

After the index job completes, reads of the same `*.ber` files are automatically parallelised across HDFS blocks.

## Structured Streaming

spark-asn1 works as a Spark Structured Streaming source. New files that arrive in the watched directory are picked up automatically.

```scala
val stream = spark.readStream
  .format("asn1")
  .option("asn1.schema",   "cdr.asn1")
  .option("asn1.type",     "CDR")
  .option("asn1.encoding", "ber")
  .load("/data/incoming/")

val query = stream
  .writeStream
  .format("parquet")
  .option("checkpointLocation", "/data/checkpoint/")
  .option("path",               "/data/output/")
  .trigger(org.apache.spark.sql.streaming.Trigger.ProcessingTime("30 seconds"))
  .start()

query.awaitTermination()
```

Files processed in previous batches are tracked by Spark's checkpoint mechanism and are not re-read.

> **Java 21+ note**: On Java 21 and later, the default checkpoint manager calls a removed API (`Subject.getSubject()`).
> If you see a related warning or error, switch to the file-system based manager:
>
> ```scala
> spark.conf.set(
>   "spark.hadoop.spark.sql.streaming.checkpointFileManagerClass",
>   "org.apache.spark.sql.execution.streaming.FileSystemBasedCheckpointFileManager"
> )
> ```

## Architecture

```
Asn1DataSource (FileFormat)
├── Schema parsing    — ANTLR4 grammar → Asn1Module ADT → SchemaRegistry
├── Type mapping      — Asn1TypeMapper: Asn1Type → Spark DataType
└── Decoding
    ├── BER/DER       — BouncyCastle ASN1StreamParser (streaming, indefinite length)
    ├── PER aligned   — PerBitBuffer + constraint-driven width calculation
    ├── PER unaligned — same, without byte-boundary alignment
    └── XER           — StAX pull parser, schema-isomorphic element mapping
```

The schema is parsed once per executor and cached in a thread-safe executor-local map (`SchemaCache`), so the parse cost is paid only once per task slot.

BER/DER decoding reads one raw TLV frame at a time, patches any REAL (tag 9) elements before handing bytes to BouncyCastle, and supports both definite-length and indefinite-length BER constructions.

## Splitting large files

By default each file is read by a single Spark task. Two strategies unlock parallelism:

**BER/DER — sidecar index**

Pre-scan the file once to record each record's byte offset. Subsequent reads split across as many tasks as Spark deems appropriate (`spark.sql.files.maxPartitionBytes`).

```scala
import io.github.sparkasn1.spark.asn1.util.Asn1Indexer
import org.apache.hadoop.fs.Path

// Run once — writes /data/cdrs.ber.asn1idx alongside the file
Asn1Indexer.buildIndex(
  new Path("/data/cdrs.ber"),
  spark.sparkContext.hadoopConfiguration
)

// From now on this read is fully parallel
val df = spark.read
  .format("asn1")
  .option("asn1.schema",   "cdr.asn1")
  .option("asn1.type",     "CDR")
  .option("asn1.encoding", "ber")
  .load("/data/cdrs.ber")
```

The index is a compact binary file (8 bytes per record). A 10-million-record file produces an ~80 MB index. The index is read on the driver and broadcast to executors, so HDFS is hit only once regardless of the number of splits.

> **Note**: indefinite-length BER constructions stop the scan early; all definite-length BER and all DER are fully supported.

**PER fixed-length — no index needed**

When `asn1.per.framing=fixed-length`, the file is automatically splittable. Spark divides it into byte-range splits and each executor aligns its slice to the nearest record boundary.

```scala
spark.read
  .format("asn1")
  .option("asn1.encoding",         "per-aligned")
  .option("asn1.per.framing",      "fixed-length")
  .option("asn1.per.record.bytes", "256")
  .load("/data/huge.per")   // automatically split across executors
```

## Write support

Write back to ASN.1 files using `df.write`:

```scala
df.write
  .format("asn1")
  .option("asn1.schema",   "person.asn1")
  .option("asn1.type",     "Person")
  .option("asn1.encoding", "ber")
  .save("/output/people.ber")
```

PER write supports the same framing options as read (`length-prefixed`, `fixed-length`, `hex-lines`).

## Troubleshooting

### Zero records decoded

If `df.count()` returns 0, or you see the log warning *"BER/DER decoder produced 0 records"*, use `Asn1Inspector.peek` to diagnose without Spark:

```scala
import io.github.sparkasn1.spark.asn1.util.Asn1Inspector

Asn1Inspector.peek(
  schemaPaths = Seq("/tmp/cdr.asn1"),
  typeName    = "PGWRecord",
  encoding    = "ber",
  filePath    = "/tmp/sample.ber"
)
```

Or from the command line:

```bash
sbt "runMain io.github.sparkasn1.spark.asn1.util.Asn1Inspector \
  --schema cdr.asn1 --type PGWRecord --encoding ber --file sample.ber"
```

Common causes:

| Symptom | Fix |
|---|---|
| Wrong `asn1.type` | Check the type name spelling; the inspector lists all available types |
| Wrong `asn1.encoding` | A BER file decoded as XER (or vice-versa) will produce 0 records |
| Schema not reachable from executors | Copy the schema to HDFS/S3, or use `--files` / `spark.files` to distribute it |
| Indefinite-length outer wrapper | The file may be a single outer SEQUENCE wrapping all records; try `asn1.type` set to the inner element type |

### Schema path warnings on clusters

If you see *"Schema path(s) … have no URI scheme"* in the Spark driver logs, your schema is specified as a bare local path (e.g. `/home/user/cdr.asn1`). Every executor needs to open that file:

```scala
// Option A — distribute via spark.files
spark.sparkContext.addFile("/home/user/cdr.asn1")
// then reference it with SparkFiles.get("cdr.asn1")

// Option B — copy to HDFS and use an HDFS URI
// spark.read.format("asn1").option("asn1.schema", "hdfs:///schemas/cdr.asn1")

// Option C — for S3
// spark.read.format("asn1").option("asn1.schema", "s3://my-bucket/schemas/cdr.asn1")
```

### Splittability

When a BER/DER file is read as a single task despite having many records, check for the log message *"No sidecar index"*. Build one with `Asn1Indexer.buildIndex` — see the [Splitting large files](#splitting-large-files) section above.

## Limitations

- PER extension additions (extension markers with unknown extensions) are skipped silently.
- Automatic tagging and explicit/implicit tag resolution are supported for BER/DER; PER and XER ignore tags by design per the standards.
- Indefinite-length BER files cannot be indexed and remain single-task reads.
- Parameterized type definitions (e.g. `My-Type{Param} ::= …`) are skipped by the parser; references to them resolve to `BinaryType` (raw bytes).
- Information object classes (`CLASS`, `DEFINED BY`, table constraints) are not supported and are silently ignored.

## License

Apache 2.0 — see [LICENSE](LICENSE).
