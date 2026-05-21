package io.github.sparkasn1.spark.asn1.codec

import io.github.sparkasn1.spark.asn1.codec.per.{AlignedPerDecoder, AlignedPerEncoder, UnalignedPerDecoder, UnalignedPerEncoder}
import io.github.sparkasn1.spark.asn1.codec.xer.{XerDecoder, XerEncoder}
import io.github.sparkasn1.spark.asn1.schema.Asn1TypeMapper
import io.github.sparkasn1.spark.asn1.util.SchemaCache
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.nio.charset.StandardCharsets

class RealTypeSpec extends AnyFlatSpec with Matchers {

  private val schemaDir  = new File(getClass.getResource("/schemas").getPath)
  private val reg        = SchemaCache.getOrParse(Seq(new File(schemaDir, "real.asn1").getAbsolutePath))
  private val asn1Type   = reg.resolve("Measurement", "RealModule").get
  private val sparkSchema = Asn1TypeMapper.toSparkType(asn1Type, reg, "RealModule").asInstanceOf[StructType]

  private def row(id: Long, value: Double, label: String) =
    new GenericInternalRow(Array[Any](id, value, UTF8String.fromString(label)))

  private val values = Seq(
    row(1L, 3.14159265358979,  "pi"),
    row(2L, -2.718281828,      "neg-e"),
    row(3L, 0.0,               "zero"),
    row(4L, 1.0e100,           "large"),
    row(5L, Double.MaxValue,   "max"),
    row(6L, Double.MinValue,   "min-positive")
  )

  private def berRoundTrip(r: GenericInternalRow): Unit = {
    val encoder = new BerDerEncoder(reg, "RealModule")
    val decoder = new BerDerDecoder(reg, "RealModule")
    val bytes   = encoder.encodeRow(r, sparkSchema, asn1Type)
    val decoded = decoder.decodeBytes(bytes, asn1Type)
    decoded.getDouble(sparkSchema.fieldIndex("value")) shouldBe
      r.getDouble(sparkSchema.fieldIndex("value")) +- 1e-10
  }

  private def aperRoundTrip(r: GenericInternalRow): Unit = {
    val bytes   = new AlignedPerEncoder(reg, "RealModule").encodeRow(r, sparkSchema, asn1Type)
    val decoded = new AlignedPerDecoder(reg, "RealModule").decodeBytes(bytes, asn1Type)
    decoded.getDouble(sparkSchema.fieldIndex("value")) shouldBe
      r.getDouble(sparkSchema.fieldIndex("value")) +- 1e-10
  }

  private def uperRoundTrip(r: GenericInternalRow): Unit = {
    val bytes   = new UnalignedPerEncoder(reg, "RealModule").encodeRow(r, sparkSchema, asn1Type)
    val decoded = new UnalignedPerDecoder(reg, "RealModule").decodeBytes(bytes, asn1Type)
    decoded.getDouble(sparkSchema.fieldIndex("value")) shouldBe
      r.getDouble(sparkSchema.fieldIndex("value")) +- 1e-10
  }

  private def xerRoundTrip(r: GenericInternalRow): Unit = {
    val bytes   = new XerEncoder(reg, "RealModule").encodeRow(r, sparkSchema, asn1Type, "Measurement")
    val xml     = new String(bytes, StandardCharsets.UTF_8).trim
    val decoded = new XerDecoder(reg, "RealModule").decodeXml(xml, asn1Type)
    decoded.getDouble(sparkSchema.fieldIndex("value")) shouldBe
      r.getDouble(sparkSchema.fieldIndex("value")) +- 1e-10
  }

  "REAL type — BER" should "round-trip finite values" in {
    values.foreach(berRoundTrip)
  }

  "REAL type — APER" should "round-trip finite values" in {
    values.foreach(aperRoundTrip)
  }

  "REAL type — UPER" should "round-trip finite values" in {
    values.foreach(uperRoundTrip)
  }

  "REAL type — XER" should "round-trip finite values" in {
    values.foreach(xerRoundTrip)
  }

  it should "encode and decode PLUS-INFINITY" in {
    val r       = row(7L, Double.PositiveInfinity, "inf")
    val bytes   = new XerEncoder(reg, "RealModule").encodeRow(r, sparkSchema, asn1Type, "Measurement")
    val xml     = new String(bytes, StandardCharsets.UTF_8)
    xml should include ("<PLUS-INFINITY/>")
    val decoded = new XerDecoder(reg, "RealModule").decodeXml(xml.trim, asn1Type)
    decoded.getDouble(sparkSchema.fieldIndex("value")) shouldBe Double.PositiveInfinity
  }

  it should "encode and decode MINUS-INFINITY" in {
    val r       = row(8L, Double.NegativeInfinity, "neginf")
    val bytes   = new XerEncoder(reg, "RealModule").encodeRow(r, sparkSchema, asn1Type, "Measurement")
    val xml     = new String(bytes, StandardCharsets.UTF_8)
    xml should include ("<MINUS-INFINITY/>")
    val decoded = new XerDecoder(reg, "RealModule").decodeXml(xml.trim, asn1Type)
    decoded.getDouble(sparkSchema.fieldIndex("value")) shouldBe Double.NegativeInfinity
  }
}
