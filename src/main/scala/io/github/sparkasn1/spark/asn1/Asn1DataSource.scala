package io.github.sparkasn1.spark.asn1

import io.github.sparkasn1.spark.asn1.Asn1DataSourceOptions.{Encoding, PerFraming}
import io.github.sparkasn1.spark.asn1.codec.BerDerDecoder
import io.github.sparkasn1.spark.asn1.codec.per.{AlignedPerDecoder, UnalignedPerDecoder}
import io.github.sparkasn1.spark.asn1.codec.xer.XerDecoder
import io.github.sparkasn1.spark.asn1.reader.{BerRecordIterator, PerRecordIterator, XerRecordIterator}
import io.github.sparkasn1.spark.asn1.schema.Asn1TypeMapper
import io.github.sparkasn1.spark.asn1.util.{Asn1Indexer, SchemaCache, SerializableHadoopConf}
import io.github.sparkasn1.spark.asn1.writer.Asn1OutputWriterFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.execution.datasources.{FileFormat, OutputWriterFactory, PartitionedFile}
import org.apache.spark.sql.sources.{DataSourceRegister, Filter}
import org.apache.spark.sql.types.{DataType, StructField, StructType}

import java.io.{FilterInputStream, InputStream}
import scala.util.Try

/**
 * Spark FileFormat data source for ASN.1-encoded files.
 *
 * Usage:
 * {{{
 *   spark.read
 *     .format("asn1")
 *     .option("asn1.schema", "/path/to/Schema.asn1")
 *     .option("asn1.type", "MyMessage")
 *     .option("asn1.encoding", "ber")  // ber | der | per-aligned | per-unaligned | xer
 *     .load("/data/messages.ber")
 * }}}
 *
 * == Splitting large files ==
 *
 * BER/DER files become splittable once a sidecar index is present:
 * {{{
 *   import io.github.sparkasn1.spark.asn1.util.Asn1Indexer
 *   Asn1Indexer.buildIndex(new Path("/data/messages.ber"), spark.sparkContext.hadoopConfiguration)
 *   // Now spark.read.format("asn1")…load("/data/messages.ber") uses multiple tasks
 * }}}
 *
 * PER fixed-length files are always splittable without an index.
 */
class Asn1DataSource extends FileFormat with DataSourceRegister {

  override def shortName(): String = "asn1"

  override def toString: String = "ASN1"

  // -----------------------------------------------------------------------
  // Splittability
  // -----------------------------------------------------------------------

  override def isSplitable(
    sparkSession: SparkSession,
    options: Map[String, String],
    path: Path
  ): Boolean = {
    val opts = Try(Asn1DataSourceOptions.parse(options)).getOrElse(return false)
    opts.encoding match {
      case Encoding.PerAligned | Encoding.PerUnaligned
        if opts.perFraming == PerFraming.FixedLength && opts.perRecordBytes.exists(_ > 0) =>
        true

      case Encoding.Ber | Encoding.Der =>
        val indexPath = new Path(path.toString + Asn1Indexer.INDEX_SUFFIX)
        Try(indexPath.getFileSystem(sparkSession.sparkContext.hadoopConfiguration)
          .exists(indexPath)).getOrElse(false)

      case _ => false
    }
  }

  // -----------------------------------------------------------------------
  // Schema inference
  // -----------------------------------------------------------------------

  override def inferSchema(
    sparkSession: SparkSession,
    options: Map[String, String],
    files: Seq[FileStatus]
  ): Option[StructType] = {
    val opts     = Asn1DataSourceOptions.parse(options)
    val registry = SchemaCache.getOrParse(opts.schemaPaths)
    val rootMod  = registry.modules.keys.headOption.getOrElse(
      throw new IllegalStateException("No modules found in the provided schema files"))
    val rootType = registry.resolve(opts.rootType, rootMod).getOrElse(
      throw new IllegalArgumentException(
        s"Type '${opts.rootType}' not found in any of the provided schema modules"))
    val sparkType: DataType = Asn1TypeMapper.toSparkType(
      rootType, registry, rootMod, opts.choiceTagField, opts.enumeratedAsInt)
    sparkType match {
      case st: StructType => Some(st)
      case other =>
        Some(StructType(Seq(StructField("value", other, nullable = true))))
    }
  }

  // -----------------------------------------------------------------------
  // Reader construction
  // -----------------------------------------------------------------------

  override def buildReader(
    sparkSession: SparkSession,
    dataSchema: StructType,
    partitionSchema: StructType,
    requiredSchema: StructType,
    filters: Seq[Filter],
    options: Map[String, String],
    hadoopConf: Configuration
  ): PartitionedFile => Iterator[InternalRow] = {

    val opts = Asn1DataSourceOptions.parse(options)

    val schemaPaths     = opts.schemaPaths
    val rootTypeName    = opts.rootType
    val encoding        = opts.encoding
    val choiceTagField  = opts.choiceTagField
    val enumeratedAsInt = opts.enumeratedAsInt
    val perFraming     = opts.perFraming match {
      case PerFraming.LengthPrefixed => "length-prefixed"
      case PerFraming.FixedLength    => "fixed-length"
      case PerFraming.HexLines       => "hex-lines"
    }
    val perRecordBytes = opts.perRecordBytes.getOrElse(0)

    // Column pruning: only decode fields requested by the query.
    // None means decode all (requiredSchema == dataSchema).
    val pruning: Option[Seq[String]] =
      if (requiredSchema.fieldNames.toSet == dataSchema.fieldNames.toSet) None
      else Some(requiredSchema.fieldNames.toSeq)

    val broadcastConf = sparkSession.sparkContext.broadcast(
      new SerializableHadoopConf(hadoopConf))

    file: PartitionedFile => {
      val registry = SchemaCache.getOrParse(schemaPaths)
      val rootMod  = registry.modules.keys.headOption.getOrElse(
        throw new IllegalStateException("No modules found in schema"))
      val rootType = registry.resolve(rootTypeName, rootMod).getOrElse(
        throw new IllegalArgumentException(s"Type '$rootTypeName' not found in schema"))

      val conf      = broadcastConf.value.get
      val path      = new Path(file.filePath.toUri)
      val fs        = path.getFileSystem(conf)
      val rawStream = fs.open(path)

      encoding match {

        // ----------------------------------------------------------------
        // BER / DER — with optional sidecar index for splitting
        // ----------------------------------------------------------------
        case Encoding.Ber | Encoding.Der =>
          val decoder   = new BerDerDecoder(registry, rootMod, enumeratedAsInt)
          val indexPath = new Path(path.toString + Asn1Indexer.INDEX_SUFFIX)
          val hasIndex  = Try(fs.exists(indexPath)).getOrElse(false)

          if (hasIndex) {
            // Binary-search the index file for this split's byte range.
            // Only the matching slice is loaded — the full index is never read into memory.
            val offsets = Asn1Indexer.readIndexSlice(
              indexPath, conf, file.start, file.start + file.length)

            if (offsets.isEmpty) {
              rawStream.close()
              (Iterator.empty: Iterator[InternalRow])
            } else {
              rawStream.seek(offsets(0))
              new BerRecordIterator(rawStream, decoder, rootType, offsets.length.toLong, pruning)
            }
          } else {
            new BerRecordIterator(rawStream, decoder, rootType, Long.MaxValue, pruning)
          }

        // ----------------------------------------------------------------
        // PER aligned / unaligned — fixed-length is splittable
        // ----------------------------------------------------------------
        case Encoding.PerAligned | Encoding.PerUnaligned =>
          val decoder = encoding match {
            case Encoding.PerAligned => new AlignedPerDecoder(registry, rootMod, enumeratedAsInt)
            case _                   => new UnalignedPerDecoder(registry, rootMod, enumeratedAsInt)
          }

          val perBase: Iterator[InternalRow] =
            if (perFraming == "fixed-length" && perRecordBytes > 0 && file.start > 0) {
              val r            = perRecordBytes.toLong
              val alignedStart = ((file.start + r - 1) / r) * r
              val alignedEnd   = ((file.start + file.length + r - 1) / r) * r
              val bytesToRead  = alignedEnd - alignedStart
              if (bytesToRead <= 0) {
                rawStream.close()
                Iterator.empty
              } else {
                rawStream.seek(alignedStart)
                new PerRecordIterator(
                  new BoundedInputStream(rawStream, bytesToRead),
                  decoder, rootType, perFraming, perRecordBytes)
              }
            } else {
              new PerRecordIterator(rawStream, decoder, rootType, perFraming, perRecordBytes)
            }
          Asn1DataSource.projectIter(perBase, dataSchema, pruning)

        // ----------------------------------------------------------------
        // XER
        // ----------------------------------------------------------------
        case Encoding.Xer =>
          val decoder = new XerDecoder(registry, rootMod, enumeratedAsInt)
          Asn1DataSource.projectIter(
            new XerRecordIterator(rawStream, decoder, rootType),
            dataSchema, pruning)

        case other =>
          rawStream.close()
          throw new UnsupportedOperationException(
            s"Encoding '$other' is not yet supported")
      }
    }
  }

  // -----------------------------------------------------------------------
  // Write support (stub)
  // -----------------------------------------------------------------------

  override def prepareWrite(
    sparkSession: SparkSession,
    job:          Job,
    options:      Map[String, String],
    dataType:     StructType
  ): OutputWriterFactory = new Asn1OutputWriterFactory(options)

  override def supportBatch(sparkSession: SparkSession, dataSchema: StructType): Boolean = false
}

private object Asn1DataSource {
  def projectIter(
    iter: Iterator[InternalRow],
    dataSchema: StructType,
    pruning: Option[Seq[String]]
  ): Iterator[InternalRow] = pruning match {
    case None => iter
    case Some(reqNames) =>
      val indices = reqNames.map(n => dataSchema.fieldIndex(n))
      iter.map { row =>
        val values = indices.map(i => row.get(i, dataSchema(i).dataType)).toArray[Any]
        new GenericInternalRow(values)
      }
  }
}

/** Limits reads to `limit` bytes; used to bound a PER fixed-length split. */
private class BoundedInputStream(underlying: InputStream, private var limit: Long)
  extends FilterInputStream(underlying) {

  override def read(): Int =
    if (limit <= 0) -1
    else { val b = underlying.read(); if (b >= 0) limit -= 1; b }

  override def read(b: Array[Byte], off: Int, len: Int): Int = {
    if (limit <= 0) return -1
    val n = underlying.read(b, off, math.min(len.toLong, limit).toInt)
    if (n > 0) limit -= n
    n
  }
}

