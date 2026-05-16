package io.github.sparkasn1.spark.asn1.writer

import io.github.sparkasn1.spark.asn1.Asn1DataSourceOptions
import io.github.sparkasn1.spark.asn1.Asn1DataSourceOptions.Encoding
import io.github.sparkasn1.spark.asn1.codec.BerDerEncoder
import io.github.sparkasn1.spark.asn1.util.SchemaCache
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{OutputWriter, OutputWriterFactory}
import org.apache.spark.sql.types.StructType

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
    val opts     = Asn1DataSourceOptions.parse(options)
    val registry = SchemaCache.getOrParse(opts.schemaPaths)
    val rootMod  = registry.modules.keys.headOption.getOrElse(
      throw new IllegalStateException("No modules found in schema"))
    val rootType = registry.resolve(opts.rootType, rootMod).getOrElse(
      throw new IllegalArgumentException(s"Type '${opts.rootType}' not found in schema"))

    opts.encoding match {
      case Encoding.Ber | Encoding.Der =>
        val encoder = new BerDerEncoder(registry, rootMod, opts.choiceTagField)
        val conf    = context.getConfiguration
        val fs      = new Path(path).getFileSystem(conf)
        val out     = fs.create(new Path(path))
        new BerOutputWriter(path, dataSchema, rootType, encoder, out)

      case other =>
        throw new UnsupportedOperationException(
          s"Write support for encoding '$other' is not yet implemented")
    }
  }
}

/** Writes successive BER/DER records to a single output file. */
private class BerOutputWriter(
  val path:   String,
  schema:     StructType,
  rootType:   io.github.sparkasn1.spark.asn1.model.Asn1Type,
  encoder:    BerDerEncoder,
  out:        org.apache.hadoop.fs.FSDataOutputStream
) extends OutputWriter {

  override def write(row: InternalRow): Unit =
    out.write(encoder.encodeRow(row, schema, rootType))

  override def close(): Unit =
    try out.close() catch { case _: Exception => () }
}
