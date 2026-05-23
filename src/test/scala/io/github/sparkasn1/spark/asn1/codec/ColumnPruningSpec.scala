package io.github.sparkasn1.spark.asn1.codec

import io.github.sparkasn1.spark.asn1.codec.xer.{XerDecoder, XerEncoder}
import io.github.sparkasn1.spark.asn1.parser.Asn1SchemaParser
import io.github.sparkasn1.spark.asn1.schema.Asn1TypeMapper
import io.github.sparkasn1.spark.asn1.util.SchemaCache
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.nio.charset.StandardCharsets

/**
 * Verifies that column pruning produces a row with only the requested fields,
 * with unrequested fields set to null — without having to decode their bytes.
 */
class ColumnPruningSpec extends AnyFlatSpec with Matchers {

  private val schemaDir  = new File(getClass.getResource("/schemas").getPath)
  private val reg        = SchemaCache.getOrParse(Seq(new File(schemaDir, "simple.asn1").getAbsolutePath))
  private val asn1Type   = reg.resolve("SimpleRecord", "SimpleModule").get
  private val fullSchema = Asn1TypeMapper.toSparkType(asn1Type, reg, "SimpleModule").asInstanceOf[StructType]

  private val sampleRow = new GenericInternalRow(Array[Any](
    42L,
    UTF8String.fromString("Alice"),
    true,
    7L,
    Array[Byte](1, 2, 3),
    UTF8String.fromString("1.2.3")
  ))

  // --------------------------------------------------------------------------
  // BER pruning
  // --------------------------------------------------------------------------

  "BerDerDecoder" should "return only requested fields when requiredSchema is set" in {
    val berBytes    = new BerDerEncoder(reg, "SimpleModule").encodeRow(sampleRow, fullSchema, asn1Type)
    val decoder     = new BerDerDecoder(reg, "SimpleModule")
    val prunedSt    = StructType(Seq(fullSchema("id"), fullSchema("name")))
    val pruned      = decoder.decodeNext(
      decoder.openParser(new java.io.ByteArrayInputStream(berBytes)),
      asn1Type,
      requiredSchema = Some(prunedSt)
    ).get

    pruned.numFields                 shouldBe 2
    pruned.getLong(0)                shouldBe 42L
    pruned.getUTF8String(1).toString shouldBe "Alice"
  }

  it should "return all fields when requiredSchema is None" in {
    val berBytes = new BerDerEncoder(reg, "SimpleModule").encodeRow(sampleRow, fullSchema, asn1Type)
    val decoder  = new BerDerDecoder(reg, "SimpleModule")
    val full     = decoder.decodeNext(
      decoder.openParser(new java.io.ByteArrayInputStream(berBytes)),
      asn1Type
    ).get

    full.numFields                 shouldBe 6
    full.getLong(0)                shouldBe 42L
    full.getUTF8String(1).toString shouldBe "Alice"
    full.getBoolean(2)             shouldBe true
  }

  it should "return only the two requested fields in the pruned row" in {
    val berBytes = new BerDerEncoder(reg, "SimpleModule").encodeRow(sampleRow, fullSchema, asn1Type)
    val decoder  = new BerDerDecoder(reg, "SimpleModule")
    val prunedSt = StructType(Seq(fullSchema("id"), fullSchema("active")))
    val pruned   = decoder.decodeNext(
      decoder.openParser(new java.io.ByteArrayInputStream(berBytes)),
      asn1Type,
      requiredSchema = Some(prunedSt)
    ).get

    pruned.numFields     shouldBe 2
    pruned.getLong(0)    shouldBe 42L
    pruned.getBoolean(1) shouldBe true
  }

  // --------------------------------------------------------------------------
  // Deep pruning — nested SEQUENCE sub-fields
  // --------------------------------------------------------------------------

  private val deepSchema =
    """DeepModule DEFINITIONS AUTOMATIC TAGS ::= BEGIN
      |  Inner ::= SEQUENCE { x INTEGER, y INTEGER, label UTF8String }
      |  Outer ::= SEQUENCE { id INTEGER, inner Inner, name UTF8String }
      |END""".stripMargin

  "BerDerDecoder deep pruning" should "decode only requested sub-fields of a nested SEQUENCE" in {
    val deepReg  = Asn1SchemaParser.parseString(deepSchema, "DeepModule")
    val schReg   = io.github.sparkasn1.spark.asn1.parser.SchemaRegistry(Seq(deepReg))
    val outerT   = schReg.resolve("Outer", "DeepModule").get
    val innerT   = schReg.resolve("Inner", "DeepModule").get
    val outerSp  = Asn1TypeMapper.toSparkType(outerT, schReg, "DeepModule").asInstanceOf[StructType]
    val innerSp  = Asn1TypeMapper.toSparkType(innerT, schReg, "DeepModule").asInstanceOf[StructType]

    // Build a full Outer row: id=7, inner={x=3, y=99, label="skip"}, name="Bob"
    val innerRow = new GenericInternalRow(Array[Any](3L, 99L, UTF8String.fromString("skip")))
    val outerRow = new GenericInternalRow(Array[Any](7L, innerRow, UTF8String.fromString("Bob")))

    val encoder  = new BerDerEncoder(schReg, "DeepModule")
    val berBytes = encoder.encodeRow(outerRow, outerSp, outerT)

    // Request only {id, inner.{x}} — y, label, and name are pruned.
    val innerPruned  = StructType(Seq(StructField("x", LongType)))
    val outerPruned  = StructType(Seq(
      StructField("id",    LongType),
      StructField("inner", innerPruned)
    ))

    val decoder = new BerDerDecoder(schReg, "DeepModule")
    val decoded = decoder.decodeNext(
      new java.io.ByteArrayInputStream(berBytes),
      outerT,
      requiredSchema = Some(outerPruned)
    ).get

    decoded.numFields shouldBe 2              // id, inner
    decoded.getLong(0) shouldBe 7L            // id

    val innerDecoded = decoded.getStruct(1, 1) // inner with 1 field (x only)
    innerDecoded.numFields shouldBe 1
    innerDecoded.getLong(0) shouldBe 3L        // x
  }

  // --------------------------------------------------------------------------
  // XER pruning
  // --------------------------------------------------------------------------

  "XerDecoder" should "skip unrequested fields when requiredNames is non-empty" in {
    val xerBytes  = new XerEncoder(reg, "SimpleModule").encodeRow(sampleRow, fullSchema, asn1Type, "SimpleRecord")
    val xml       = new String(xerBytes, StandardCharsets.UTF_8).trim
    val decoder   = new XerDecoder(reg, "SimpleModule")

    val pruned    = decoder.decodeXml(xml, asn1Type, requiredNames = Set("id", "name"))

    pruned.numFields shouldBe 6  // row has all 6 slots; unrequested are null
    pruned.getLong(fullSchema.fieldIndex("id"))                shouldBe 42L
    pruned.getUTF8String(fullSchema.fieldIndex("name")).toString shouldBe "Alice"
    pruned.isNullAt(fullSchema.fieldIndex("active"))           shouldBe true
    pruned.isNullAt(fullSchema.fieldIndex("score"))            shouldBe true
    pruned.isNullAt(fullSchema.fieldIndex("data"))             shouldBe true
    pruned.isNullAt(fullSchema.fieldIndex("oid"))              shouldBe true
  }

  it should "decode all fields when requiredNames is empty" in {
    val xerBytes = new XerEncoder(reg, "SimpleModule").encodeRow(sampleRow, fullSchema, asn1Type, "SimpleRecord")
    val xml      = new String(xerBytes, StandardCharsets.UTF_8).trim
    val decoder  = new XerDecoder(reg, "SimpleModule")

    val full = decoder.decodeXml(xml, asn1Type)

    full.numFields                             shouldBe 6
    full.getLong(fullSchema.fieldIndex("id"))  shouldBe 42L
    full.getBoolean(fullSchema.fieldIndex("active")) shouldBe true
  }
}
