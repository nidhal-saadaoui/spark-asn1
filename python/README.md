# pyspark-asn1

[![PyPI](https://img.shields.io/pypi/v/pyspark-asn1)](https://pypi.org/project/pyspark-asn1/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/nidhal-saadaoui/spark-asn1/blob/main/LICENSE)

PySpark integration for [spark-asn1](https://github.com/nidhal-saadaoui/spark-asn1) — a schema-driven Apache Spark data source for reading ASN.1-encoded files (BER, DER, Aligned PER, Unaligned PER, XER) without any code-generation step.

## Installation

```bash
pip install pyspark-asn1
```

## Quick start

```python
import pyspark_asn1
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Register the spark-asn1 JAR with the active session
pyspark_asn1.register(spark)

# Read ASN.1-encoded files exactly like any other Spark format
df = (spark.read
      .format("asn1")
      .option("asn1.schema",   "/path/to/schema.asn1")
      .option("asn1.type",     "MyMessage")
      .option("asn1.encoding", "ber")          # ber | der | per-aligned | per-unaligned | xer
      .load("/data/messages/*.ber"))

df.printSchema()
df.show()
```

## Options

| Option | Default | Description |
|---|---|---|
| `asn1.schema` | required | Path(s) to `.asn1` schema files (comma-separated) |
| `asn1.type` | required | Root ASN.1 type name to decode |
| `asn1.encoding` | `ber` | `ber`, `der`, `per-aligned`, `per-unaligned`, `xer` |
| `asn1.per.framing` | `length-prefixed` | PER framing: `length-prefixed`, `fixed-length`, `hex-lines` |
| `asn1.per.record.bytes` | — | Record size for fixed-length PER framing |
| `asn1.choice.tag.field` | `_tag` | Discriminator field name for CHOICE types |
| `asn1.enumerated.as.int` | `false` | Return ENUMERATED as integer instead of name |

## CHOICE types

ASN.1 CHOICE maps to a struct with a `_tag` discriminator plus one nullable field per alternative:

```python
# CHOICE { circle Circle, rectangle Rectangle }
# → schema: _tag STRING, circle STRUCT<…>, rectangle STRUCT<…>

from pyspark.sql.functions import col, when

df.filter(col("_tag") == "circle").select(col("circle.*")).show()

df.select(
    when(col("_tag") == "circle",    col("circle.radius"))
    .when(col("_tag") == "rectangle", col("rectangle.width"))
    .alias("dimension")
).show()
```

## Parallel reads (BER/DER)

Pre-scan a file once to enable parallel Spark tasks:

```python
# Run once — writes a sidecar .asn1idx file
from py4j.java_gateway import java_import
java_import(spark._jvm, "io.github.sparkasn1.spark.asn1.util.Asn1Indexer")
java_import(spark._jvm, "org.apache.hadoop.fs.Path")

Asn1Indexer = spark._jvm.io.github.sparkasn1.spark.asn1.util.Asn1Indexer
path = spark._jvm.org.apache.hadoop.fs.Path("/data/messages.ber")
Asn1Indexer.buildIndex(path, spark._jsc.hadoopConfiguration())

# Subsequent reads are fully parallel
df = spark.read.format("asn1").option("asn1.schema", "schema.asn1") \
    .option("asn1.type", "MyMessage").load("/data/messages.ber")
```

## Links

- [Full documentation & Scala API](https://nidhal-saadaoui.github.io/spark-asn1/)
- [Source code](https://github.com/nidhal-saadaoui/spark-asn1)
- [Issue tracker](https://github.com/nidhal-saadaoui/spark-asn1/issues)
- [Maven Central](https://central.sonatype.com/artifact/io.github.nidhal-saadaoui/spark-asn1_2.13)
