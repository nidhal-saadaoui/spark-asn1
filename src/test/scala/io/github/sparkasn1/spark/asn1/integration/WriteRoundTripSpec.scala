package io.github.sparkasn1.spark.asn1.integration

import io.github.sparkasn1.spark.asn1.writer.Asn1OutputWriterFactory
import io.github.sparkasn1.spark.asn1.codec.BerDerDecoder
import io.github.sparkasn1.spark.asn1.codec.per.{AlignedPerDecoder, UnalignedPerDecoder}
import io.github.sparkasn1.spark.asn1.codec.xer.XerDecoder
import io.github.sparkasn1.spark.asn1.reader.{BerRecordIterator, PerRecordIterator, XerRecordIterator}
import io.github.sparkasn1.spark.asn1.schema.Asn1TypeMapper
import io.github.sparkasn1.spark.asn1.util.SchemaCache
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.{File, FileInputStream, FileOutputStream}
import java.nio.file.Files

/**
 * Round-trip write → read tests for BER, APER, UPER, and XER output writers.
 *
 * Uses newInstanceFromStream to bypass Hadoop FileSystem (incompatible with Java 25+).
 */
class WriteRoundTripSpec extends AnyFlatSpec with Matchers {

  private val schemaFile  = new File(getClass.getResource("/schemas/simple.asn1").getPath)
  private val schemaPath  = schemaFile.getAbsolutePath
  private val reg         = SchemaCache.getOrParse(Seq(schemaPath))
  private val modName     = "SimpleModule"
  private val asn1Type    = reg.resolve("PersonRecord", modName).get
  private val sparkSchema =
    Asn1TypeMapper.toSparkType(asn1Type, reg, modName).asInstanceOf[StructType]

  // Three rows: all fields, optional age absent, optional email absent
  private val rows = Seq(
    new GenericInternalRow(Array[Any](
      UTF8String.fromString("Alice"), UTF8String.fromString("Smith"),
      30L, UTF8String.fromString("a@example.com"))),
    new GenericInternalRow(Array[Any](
      UTF8String.fromString("Bob"), UTF8String.fromString("Jones"),
      null, UTF8String.fromString("b@example.com"))),
    new GenericInternalRow(Array[Any](
      UTF8String.fromString("Carol"), UTF8String.fromString("White"),
      25L, null))
  )

  private def baseOptions(encoding: String) = Map(
    "asn1.schema"   -> schemaPath,
    "asn1.type"     -> "PersonRecord",
    "asn1.encoding" -> encoding
  )

  private def tmpFile(name: String): File =
    new File(Files.createTempDirectory("spark-asn1-write-test").toFile, name)

  private def writeRows(factory: Asn1OutputWriterFactory, outFile: File): Unit = {
    val fos    = new FileOutputStream(outFile)
    val writer = factory.newInstanceFromStream(outFile.getAbsolutePath, sparkSchema, fos)
    rows.foreach(writer.write)
    writer.close()
  }

  private def checkRows(decoded: Seq[GenericInternalRow]): Unit = {
    decoded should have size 3
    val fn = sparkSchema.fieldIndex("firstName")
    val ln = sparkSchema.fieldIndex("lastName")
    val ag = sparkSchema.fieldIndex("age")
    val em = sparkSchema.fieldIndex("email")

    decoded(0).getUTF8String(fn).toString shouldBe "Alice"
    decoded(0).getUTF8String(ln).toString shouldBe "Smith"
    decoded(0).getLong(ag)                shouldBe 30L
    decoded(0).getUTF8String(em).toString shouldBe "a@example.com"

    decoded(1).getUTF8String(fn).toString shouldBe "Bob"
    decoded(1).isNullAt(ag)               shouldBe true
    decoded(1).getUTF8String(em).toString shouldBe "b@example.com"

    decoded(2).getLong(ag)                shouldBe 25L
    decoded(2).isNullAt(em)               shouldBe true
  }

  // -------------------------------------------------------------------------

  "Asn1OutputWriter — BER" should "round-trip PersonRecord rows" in {
    val outFile = tmpFile("out.ber")
    writeRows(new Asn1OutputWriterFactory(baseOptions("ber")), outFile)

    val decoder = new BerDerDecoder(reg, modName)
    val is      = new FileInputStream(outFile)
    try {
      val decoded = new BerRecordIterator(is, decoder, asn1Type, Long.MaxValue, None)
        .toSeq.map(_.asInstanceOf[GenericInternalRow])
      checkRows(decoded)
    } finally is.close()
  }

  "Asn1OutputWriter — APER" should "round-trip PersonRecord rows" in {
    val outFile = tmpFile("out.per")
    val opts    = baseOptions("per-aligned") + ("asn1.per.framing" -> "length-prefixed")
    writeRows(new Asn1OutputWriterFactory(opts), outFile)

    val decoder = new AlignedPerDecoder(reg, modName)
    val is      = new FileInputStream(outFile)
    try {
      val decoded = new PerRecordIterator(is, decoder, asn1Type, "length-prefixed", 0)
        .toSeq.map(_.asInstanceOf[GenericInternalRow])
      checkRows(decoded)
    } finally is.close()
  }

  "Asn1OutputWriter — UPER" should "round-trip PersonRecord rows" in {
    val outFile = tmpFile("out.per")
    val opts    = baseOptions("per-unaligned") + ("asn1.per.framing" -> "length-prefixed")
    writeRows(new Asn1OutputWriterFactory(opts), outFile)

    val decoder = new UnalignedPerDecoder(reg, modName)
    val is      = new FileInputStream(outFile)
    try {
      val decoded = new PerRecordIterator(is, decoder, asn1Type, "length-prefixed", 0)
        .toSeq.map(_.asInstanceOf[GenericInternalRow])
      checkRows(decoded)
    } finally is.close()
  }

  "Asn1OutputWriter — XER" should "round-trip PersonRecord rows" in {
    val outFile = tmpFile("out.xer")
    writeRows(new Asn1OutputWriterFactory(baseOptions("xer")), outFile)

    val decoder = new XerDecoder(reg, modName)
    val is      = new FileInputStream(outFile)
    try {
      val decoded = new XerRecordIterator(is, decoder, asn1Type)
        .toSeq.map(_.asInstanceOf[GenericInternalRow])
      checkRows(decoded)
    } finally is.close()
  }

}
