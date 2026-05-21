package io.github.sparkasn1.spark.asn1.writer

import io.github.sparkasn1.spark.asn1.Asn1DataSourceOptions
import io.github.sparkasn1.spark.asn1.Asn1DataSourceOptions.{Encoding, PerFraming}
import io.github.sparkasn1.spark.asn1.codec.BerDerEncoder
import io.github.sparkasn1.spark.asn1.codec.per.{AlignedPerEncoder, UnalignedPerEncoder}
import io.github.sparkasn1.spark.asn1.codec.xer.XerEncoder
import io.github.sparkasn1.spark.asn1.util.SchemaCache
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{OutputWriter, OutputWriterFactory}
import org.apache.spark.sql.types.StructType

import java.io.OutputStream

/**
 * OutputWriterFactory for ASN.1 BER/DER output.
 * One instance is created on the driver in `prepareWrite` and serialized to executors.
 * Each executor calls `newInstance` per output file.
 */
class Asn1OutputWriterFactory(options: Map[String, String]) extends OutputWriterFactory {

  override def getFileExtension(context: TaskAttemptContext): String = {
    val opts = Asn1DataSourceOptions.parse(options)
    opts.encoding match {
      case Encoding.Ber | Encoding.Der => ".ber"
      case Encoding.Xer               => ".xer"
      case Encoding.PerAligned        => ".per"
      case Encoding.PerUnaligned      => ".per"
    }
  }

  override def newInstance(
    path:       String,
    dataSchema: StructType,
    context:    TaskAttemptContext
  ): OutputWriter = {
    val conf = context.getConfiguration
    val out  = new Path(path).getFileSystem(conf).create(new Path(path))
    newInstanceFromStream(path, dataSchema, out)
  }

  /**
   * Create an OutputWriter that writes to an existing OutputStream.
   * Used in tests to bypass Hadoop FileSystem (incompatible with Java 25+).
   */
  def newInstanceFromStream(
    path:       String,
    dataSchema: StructType,
    out:        OutputStream
  ): OutputWriter = {
    val opts     = Asn1DataSourceOptions.parse(options)
    val registry = SchemaCache.getOrParse(opts.schemaPaths)
    val rootMod  = registry.modules.keys.headOption.getOrElse(
      throw new IllegalStateException("No modules found in schema"))
    val rootType = registry.resolve(opts.rootType, rootMod).getOrElse(
      throw new IllegalArgumentException(s"Type '${opts.rootType}' not found in schema"))

    opts.encoding match {
      case Encoding.Ber | Encoding.Der =>
        val encoder = new BerDerEncoder(registry, rootMod, opts.choiceTagField)
        new BerOutputWriter(path, dataSchema, rootType, encoder, out)

      case Encoding.PerAligned | Encoding.PerUnaligned =>
        val encoder = opts.encoding match {
          case Encoding.PerAligned => new AlignedPerEncoder(registry, rootMod, opts.choiceTagField)
          case _                   => new UnalignedPerEncoder(registry, rootMod, opts.choiceTagField)
        }
        new PerOutputWriter(path, dataSchema, rootType, encoder, out, opts.perFraming)

      case Encoding.Xer =>
        val encoder = new XerEncoder(registry, rootMod, opts.choiceTagField)
        new XerOutputWriter(path, dataSchema, rootType, encoder, opts.rootType, out)
    }
  }
}

/** Writes successive BER/DER records to a single output file. */
private class BerOutputWriter(
  val path:   String,
  schema:     StructType,
  rootType:   io.github.sparkasn1.spark.asn1.model.Asn1Type,
  encoder:    BerDerEncoder,
  out:        OutputStream
) extends OutputWriter {

  override def write(row: InternalRow): Unit =
    out.write(encoder.encodeRow(row, schema, rootType))

  override def close(): Unit =
    try out.close() catch { case _: Exception => () }
}

/**
 * Writes PER records. Framing options:
 *   length-prefixed (default): 4-byte big-endian length + payload
 *   fixed-length: raw bytes
 *   hex-lines: hex-encoded payload + newline
 */
private class PerOutputWriter(
  val path:   String,
  schema:     StructType,
  rootType:   io.github.sparkasn1.spark.asn1.model.Asn1Type,
  encoder:    io.github.sparkasn1.spark.asn1.codec.per.PerEncoder,
  out:        OutputStream,
  framing:    PerFraming
) extends OutputWriter {

  override def write(row: InternalRow): Unit = {
    val perBytes = encoder.encodeRow(row, schema, rootType)
    framing match {
      case PerFraming.FixedLength =>
        out.write(perBytes)
      case PerFraming.HexLines =>
        val hex = perBytes.map(b => f"${b & 0xff}%02x").mkString
        out.write((hex + "\n").getBytes("UTF-8"))
      case _ =>  // length-prefixed (default)
        val len = perBytes.length
        out.write(Array[Byte](
          ((len >> 24) & 0xff).toByte,
          ((len >> 16) & 0xff).toByte,
          ((len >>  8) & 0xff).toByte,
          ( len        & 0xff).toByte
        ))
        out.write(perBytes)
    }
  }

  override def close(): Unit =
    try out.close() catch { case _: Exception => () }
}

/** Writes successive XER records (each a `<rootTag>…</rootTag>` line) to an output file. */
private class XerOutputWriter(
  val path:   String,
  schema:     StructType,
  rootType:   io.github.sparkasn1.spark.asn1.model.Asn1Type,
  encoder:    XerEncoder,
  rootTag:    String,
  out:        OutputStream
) extends OutputWriter {

  override def write(row: InternalRow): Unit =
    out.write(encoder.encodeRow(row, schema, rootType, rootTag))

  override def close(): Unit =
    try out.close() catch { case _: Exception => () }
}
