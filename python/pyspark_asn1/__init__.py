"""
pyspark-asn1 — zero-friction PySpark integration for spark-asn1.

Usage::

    import pyspark_asn1

    pyspark_asn1.register(spark)

    df = (spark.read
          .format("asn1")
          .option("asn1.schema",   "/path/to/schema.asn1")
          .option("asn1.type",     "MyMessage")
          .option("asn1.encoding", "ber")
          .load("/data/messages.ber"))
"""

import os

try:
    from ._version import version as __version__
except ImportError:
    __version__ = "unknown"


def register(spark):
    """Add the spark-asn1 assembly JAR to *spark* so the ``asn1`` format is available.

    Parameters
    ----------
    spark:
        An active ``pyspark.sql.SparkSession``.

    Example
    -------
    >>> import pyspark_asn1
    >>> pyspark_asn1.register(spark)
    >>> df = spark.read.format("asn1") \\
    ...         .option("asn1.schema",   "schema.asn1") \\
    ...         .option("asn1.type",     "MyType") \\
    ...         .option("asn1.encoding", "ber") \\
    ...         .load("data.ber")
    """
    spark.sparkContext.addJar(jar_path())


def jar_path() -> str:
    """Return the absolute path to the bundled spark-asn1 assembly JAR."""
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "spark-asn1-assembly.jar")
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"Assembly JAR not found at {path}. "
            "This is a packaging error — please file an issue at "
            "https://github.com/nidhal-saadaoui/spark-asn1/issues"
        )
    return path
