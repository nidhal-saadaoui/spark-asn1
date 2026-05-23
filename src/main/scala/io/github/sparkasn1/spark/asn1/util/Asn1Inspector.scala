package io.github.sparkasn1.spark.asn1.util

import io.github.sparkasn1.spark.asn1.codec.BerDerDecoder
import io.github.sparkasn1.spark.asn1.codec.per.{AlignedPerDecoder, UnalignedPerDecoder}
import io.github.sparkasn1.spark.asn1.codec.xer.XerDecoder
import io.github.sparkasn1.spark.asn1.model.Asn1Type
import io.github.sparkasn1.spark.asn1.reader.{BerRecordIterator, PerRecordIterator, XerRecordIterator}
import io.github.sparkasn1.spark.asn1.schema.Asn1TypeMapper
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import java.io.FileInputStream

/**
 * Lightweight diagnostic utility — decode the first few records from a local
 * ASN.1 file without a SparkSession.
 *
 * Typical use: paste into a notebook or `sbt console` to verify your options
 * before submitting a full Spark job.
 *
 * {{{
 *   import io.github.sparkasn1.spark.asn1.util.Asn1Inspector
 *   Asn1Inspector.peek(
 *     schemaPaths = Seq("/tmp/cdr.asn1"),
 *     typeName    = "PGWRecord",
 *     encoding    = "ber",
 *     filePath    = "/tmp/sample.ber"
 *   )
 * }}}
 *
 * Can also be run from the command line:
 * {{{
 *   sbt "runMain io.github.sparkasn1.spark.asn1.util.Asn1Inspector \
 *     --schema cdr.asn1 --type PGWRecord --encoding ber --file sample.ber"
 * }}}
 */
object Asn1Inspector {

  /**
   * Decode up to `maxRecords` records from `filePath` and return a
   * human-readable report string.  The report is also printed to stdout.
   *
   * @param schemaPaths paths to one or more `.asn1` schema files
   * @param typeName    root ASN.1 type name to decode
   * @param encoding    one of: ber, der, per-aligned, per-unaligned, xer
   * @param filePath    local file path to read
   * @param maxRecords  maximum records to decode (default 3)
   * @param perFraming  PER framing mode: length-prefixed (default), fixed-length, hex-lines
   * @param perRecordBytes record size for fixed-length PER framing
   * @return the formatted report string
   */
  def peek(
    schemaPaths:    Seq[String],
    typeName:       String,
    encoding:       String      = "ber",
    filePath:       String,
    maxRecords:     Int         = 3,
    perFraming:     String      = "length-prefixed",
    perRecordBytes: Int         = 0
  ): String = {
    val registry = SchemaCache.getOrParse(schemaPaths)
    val modName  = registry.modules.keys.headOption.getOrElse(
      return "ERROR: no modules found in schema files")
    val asn1Type = registry.resolve(typeName, modName).getOrElse(
      return s"ERROR: type '$typeName' not found in schema. " +
        s"Available types: ${registry.modules.values.flatMap(_.typeAssignments.keys).toSeq.sorted.mkString(", ")}")

    val sparkType = Asn1TypeMapper.toSparkType(asn1Type, registry, modName) match {
      case st: StructType => st
      case other          => StructType(Seq(StructField("value", other, nullable = true)))
    }

    val rows = decodeFirst(schemaPaths, typeName, encoding, filePath, asn1Type, registry, modName,
      maxRecords, perFraming, perRecordBytes)

    val header =
      s"=== Asn1Inspector — ${schemaPaths.map(p => new java.io.File(p).getName).mkString(", ")}" +
      s" / $typeName ($encoding) ===\nFile: $filePath\n"

    val body = if (rows.isEmpty) {
      "\nDecoded 0 records.\n\n" +
      "Possible causes:\n" +
      "  • Wrong asn1.type — check type name spelling and module\n" +
      "  • Wrong asn1.encoding — BER files won't parse as XER and vice-versa\n" +
      s"  • Available types: ${registry.modules.values.flatMap(_.typeAssignments.keys).toSeq.sorted.mkString(", ")}\n"
    } else {
      rows.zipWithIndex.map { case (row, i) =>
        s"\nRecord ${i + 1}:\n${formatRow(row, sparkType, "  ")}"
      }.mkString("\n") + s"\n\n(decoded ${rows.length} record(s))\n"
    }

    val report = header + body
    print(report)
    report
  }

  // -------------------------------------------------------------------------
  // Decoder dispatch
  // -------------------------------------------------------------------------

  private def decodeFirst(
    schemaPaths:    Seq[String],
    typeName:       String,
    encoding:       String,
    filePath:       String,
    asn1Type:       Asn1Type,
    registry:       io.github.sparkasn1.spark.asn1.parser.SchemaRegistry,
    modName:        String,
    maxRecords:     Int,
    perFraming:     String,
    perRecordBytes: Int
  ): Seq[InternalRow] = {
    val is = new FileInputStream(filePath)
    try {
      encoding.toLowerCase match {
        case "ber" | "der" =>
          val dec  = new BerDerDecoder(registry, modName)
          val iter = new BerRecordIterator(is, dec, asn1Type, maxRecords.toLong,
            sourceName = new java.io.File(filePath).getName)
          iter.take(maxRecords).toSeq

        case "per-aligned" =>
          val dec  = new AlignedPerDecoder(registry, modName)
          new PerRecordIterator(is, dec, asn1Type, perFraming, perRecordBytes)
            .take(maxRecords).toSeq

        case "per-unaligned" =>
          val dec  = new UnalignedPerDecoder(registry, modName)
          new PerRecordIterator(is, dec, asn1Type, perFraming, perRecordBytes)
            .take(maxRecords).toSeq

        case "xer" =>
          val dec  = new XerDecoder(registry, modName)
          new XerRecordIterator(is, dec, asn1Type, Set.empty)
            .take(maxRecords).toSeq

        case other =>
          println(s"ERROR: unknown encoding '$other'. Use: ber, der, per-aligned, per-unaligned, xer")
          Seq.empty
      }
    } finally is.close()
  }

  // -------------------------------------------------------------------------
  // Formatting
  // -------------------------------------------------------------------------

  private def formatRow(row: InternalRow, schema: StructType, indent: String): String = {
    val lengths   = schema.fields.map(_.name.length)
    val nameWidth = if (lengths.isEmpty) 0 else lengths.max
    schema.fields.zipWithIndex.map { case (f, i) =>
      val v   = row.get(i, f.dataType)
      val pad = " " * (nameWidth - f.name.length)
      s"$indent${f.name}$pad : ${formatValue(v, f.dataType, indent)}"
    }.mkString("\n")
  }

  private def formatValue(value: Any, dt: DataType, indent: String): String = value match {
    case null                => "null"
    case b: Boolean          => b.toString
    case l: Long             => l.toString
    case d: Double           => d.toString
    case s: UTF8String       => s""""${s.toString}""""
    case bytes: Array[Byte]  =>
      if (bytes.length <= 16) "0x" + bytes.map("%02x".format(_)).mkString
      else s"0x${bytes.take(16).map("%02x".format(_)).mkString}… (${bytes.length} bytes)"
    case row: InternalRow    => dt match {
      case st: StructType =>
        val inner = indent + "  "
        s"{\n${formatRow(row, st, inner)}\n$indent}"
      case _ => row.toString
    }
    case arr: ArrayData      => dt match {
      case at: ArrayType =>
        val elems = (0 until arr.numElements()).map { i =>
          formatValue(arr.get(i, at.elementType), at.elementType, indent)
        }
        if (elems.isEmpty) "[]"
        else if (elems.length <= 5) s"[${elems.mkString(", ")}]"
        else s"[${elems.take(5).mkString(", ")}, … (${elems.length} total)]"
      case _ => arr.toString
    }
    case other               => other.toString
  }

  // -------------------------------------------------------------------------
  // CLI entry point
  // -------------------------------------------------------------------------

  def main(args: Array[String]): Unit = {
    val argMap = args.sliding(2, 2).collect { case Array(k, v) => k -> v }.toMap

    def get(long: String, short: String, default: String = "") =
      argMap.getOrElse(long, argMap.getOrElse(short, default))

    val schema   = get("--schema",   "-s")
    val typeName = get("--type",     "-t")
    val enc      = get("--encoding", "-e", "ber")
    val file     = get("--file",     "-f")
    val n        = scala.util.Try(get("--count",        "-n", "3").toInt).getOrElse(3)
    val framing  = get("--framing",  "-F", "length-prefixed")
    val recBytes = scala.util.Try(get("--record-bytes", "-r", "0").toInt).getOrElse(0)

    if (schema.isEmpty || typeName.isEmpty || file.isEmpty) {
      println(
        """Usage:
          |  Asn1Inspector --schema <path.asn1> --type <TypeName> --file <data.ber>
          |                [--encoding ber|der|per-aligned|per-unaligned|xer]
          |                [--count N]
          |                [--framing length-prefixed|fixed-length|hex-lines]
          |                [--record-bytes N]""".stripMargin)
      sys.exit(1)
    }

    peek(
      schemaPaths    = schema.split(',').map(_.trim).toSeq,
      typeName       = typeName,
      encoding       = enc,
      filePath       = file,
      maxRecords     = n,
      perFraming     = framing,
      perRecordBytes = recBytes
    )
  }
}
