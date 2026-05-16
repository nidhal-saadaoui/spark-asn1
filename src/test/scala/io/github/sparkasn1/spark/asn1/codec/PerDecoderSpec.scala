package io.github.sparkasn1.spark.asn1.codec

import io.github.sparkasn1.spark.asn1.codec.per.{AlignedPerDecoder, PerBitBuffer, UnalignedPerDecoder}
import io.github.sparkasn1.spark.asn1.model._
import io.github.sparkasn1.spark.asn1.parser.SchemaRegistry
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Unit tests for PER decoding.
 *
 * All byte patterns are hand-crafted per X.691.
 * Helper `bits(s)` converts a binary string to a byte array, making the
 * encoding visible at a glance ("10000001 01000001" → 2 bytes).
 */
class PerDecoderSpec extends AnyFlatSpec with Matchers {

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  private val dummyRegistry = SchemaRegistry(Seq.empty)

  private def aligned   = new AlignedPerDecoder(dummyRegistry, "Test")
  private def unaligned = new UnalignedPerDecoder(dummyRegistry, "Test")

  /** Parse a binary-string (spaces ignored) into a byte array. */
  private def bits(s: String): Array[Byte] = {
    val clean = s.replaceAll("\\s+", "")
    require(clean.length % 8 == 0, s"bit string length not a multiple of 8: ${clean.length}")
    clean.grouped(8).map(g => java.lang.Integer.parseInt(g, 2).toByte).toArray
  }

  // -------------------------------------------------------------------------
  // PerBitBuffer
  // -------------------------------------------------------------------------

  "PerBitBuffer" should "read individual bits MSB-first" in {
    val buf = PerBitBuffer(bits("10110000"))
    buf.readBit() shouldBe 1
    buf.readBit() shouldBe 0
    buf.readBit() shouldBe 1
    buf.readBit() shouldBe 1
    buf.readBit() shouldBe 0
  }

  it should "read n bits as Long" in {
    val buf = PerBitBuffer(bits("10110000"))
    buf.readBits(4) shouldBe 0xbL  // 1011
    buf.readBits(4) shouldBe 0x0L  // 0000
  }

  it should "align to the next byte boundary" in {
    val buf = PerBitBuffer(bits("10110000 11001100"))
    buf.readBits(3)  // consume 3 bits
    buf.align()      // advance to bit 8
    buf.readBits(8) shouldBe 0xccL
  }

  // -------------------------------------------------------------------------
  // BOOLEAN
  // -------------------------------------------------------------------------

  "PerDecoder (unaligned)" should "decode BOOLEAN from 1 bit" in {
    // UPER BOOLEAN: 1 bit (1=true, 0=false), packed
    val bytes = bits("10000000") // bit 0 = 1 (TRUE), rest padding
    val row   = unaligned.decodeBytes(bytes, Asn1Boolean)
    row.getBoolean(0) shouldBe true
  }

  "PerDecoder (aligned)" should "decode BOOLEAN from 1 byte" in {
    // APER BOOLEAN: 1 byte, non-zero = TRUE
    val bytes = bits("11111111")
    aligned.decodeBytes(bytes, Asn1Boolean).getBoolean(0) shouldBe true

    val bytes2 = bits("00000000")
    aligned.decodeBytes(bytes2, Asn1Boolean).getBoolean(0) shouldBe false
  }

  // -------------------------------------------------------------------------
  // NULL
  // -------------------------------------------------------------------------

  "PerDecoder" should "decode NULL consuming 0 bits" in {
    val bytes = bits("00000000")
    val row   = aligned.decodeBytes(bytes, Asn1Null)
    row.isNullAt(0) shouldBe true
  }

  // -------------------------------------------------------------------------
  // INTEGER — constrained
  // -------------------------------------------------------------------------

  "PerDecoder (unaligned)" should "decode constrained INTEGER(0..3) in 2 bits" in {
    // value 2 encoded in 2 bits = 10, left-justified in byte = 10000000
    val bytes = bits("10000000")
    val schema = Asn1Integer(Some(ValueRangeConstraint(0, 3)))
    unaligned.decodeBytes(bytes, schema).getLong(0) shouldBe 2L
  }

  it should "decode constrained INTEGER(1..8) in 3 bits, offset by min" in {
    // range 1..8 → 3 bits, value 5 → offset 4 → encoded 100
    val bytes = bits("10000000")
    val schema = Asn1Integer(Some(ValueRangeConstraint(1, 8)))
    unaligned.decodeBytes(bytes, schema).getLong(0) shouldBe 5L
  }

  it should "decode constrained INTEGER(0..255) in 8 bits" in {
    val bytes = bits("10101010")
    val schema = Asn1Integer(Some(ValueRangeConstraint(0, 255)))
    unaligned.decodeBytes(bytes, schema).getLong(0) shouldBe 0xaaL
  }

  "PerDecoder (aligned)" should "decode constrained INTEGER(0..255) in 8 bits (aligned)" in {
    val bytes = bits("01000001") // 65 = 'A'
    val schema = Asn1Integer(Some(ValueRangeConstraint(0, 255)))
    aligned.decodeBytes(bytes, schema).getLong(0) shouldBe 65L
  }

  // -------------------------------------------------------------------------
  // INTEGER — unconstrained
  // -------------------------------------------------------------------------

  it should "decode unconstrained INTEGER via length-determinant" in {
    // Length = 1 byte (0x01), value = 0x2a (42)
    val bytes = bits("00000001 00101010")
    aligned.decodeBytes(bytes, Asn1Integer()).getLong(0) shouldBe 42L
  }

  it should "decode unconstrained negative INTEGER" in {
    // Length = 1, value = 0xff = -1 (two's complement)
    val bytes = bits("00000001 11111111")
    aligned.decodeBytes(bytes, Asn1Integer()).getLong(0) shouldBe -1L
  }

  // -------------------------------------------------------------------------
  // ENUMERATED
  // -------------------------------------------------------------------------

  "PerDecoder" should "decode ENUMERATED value by index" in {
    val schema = Asn1Enumerated(Seq(
      EnumerationItem("red",   Some(0)),
      EnumerationItem("green", Some(1)),
      EnumerationItem("blue",  Some(2))
    ))
    // 2 values need 2 bits; index 1 = "green" → bits 01, padded: 01000000
    val bytes = bits("01000000")
    unaligned.decodeBytes(bytes, schema).getString(0) shouldBe "green"
  }

  // -------------------------------------------------------------------------
  // OCTET STRING
  // -------------------------------------------------------------------------

  "PerDecoder" should "decode unconstrained OCTET STRING" in {
    // Length = 3, then bytes 0x01 0x02 0x03
    val bytes = bits("00000011 00000001 00000010 00000011")
    val row   = aligned.decodeBytes(bytes, Asn1OctetString)
    row.getBinary(0) shouldBe Array[Byte](1, 2, 3)
  }

  // -------------------------------------------------------------------------
  // SEQUENCE
  // -------------------------------------------------------------------------

  "PerDecoder (unaligned)" should "decode a simple SEQUENCE" in {
    // Schema: SEQUENCE { a INTEGER(0..3), b BOOLEAN }
    // a=2 → 2 bits (10), b=true → 1 bit (1)
    // packed: 10 1 00000 = 10100000
    val schema = Asn1Sequence(Seq(
      ComponentType("a", Asn1Integer(Some(ValueRangeConstraint(0, 3))), optional = false, default = None),
      ComponentType("b", Asn1Boolean, optional = false, default = None)
    ))
    val bytes = bits("10100000")
    val row   = unaligned.decodeBytes(bytes, schema)
    row.getLong(0)    shouldBe 2L
    row.getBoolean(1) shouldBe true
  }

  "PerDecoder (unaligned)" should "decode SEQUENCE with OPTIONAL field present" in {
    // Schema: SEQUENCE { a INTEGER(0..1), b BOOLEAN OPTIONAL }
    // presence bitmap: 1 bit (b present=1)
    // a=1 → 1 bit (1), b=true → 1 bit (1)
    // packed: 1 1 1 00000 = 11100000
    val schema = Asn1Sequence(Seq(
      ComponentType("a", Asn1Integer(Some(ValueRangeConstraint(0, 1))), optional = false, default = None),
      ComponentType("b", Asn1Boolean, optional = true, default = None)
    ))
    val bytes = bits("11100000")
    val row   = unaligned.decodeBytes(bytes, schema)
    row.getLong(0)    shouldBe 1L
    row.getBoolean(1) shouldBe true
  }

  "PerDecoder (unaligned)" should "decode SEQUENCE with OPTIONAL field absent" in {
    // presence bitmap: 1 bit (b absent=0), a=1 → 1 bit (1)
    // packed: 0 1 000000 = 01000000
    val schema = Asn1Sequence(Seq(
      ComponentType("a", Asn1Integer(Some(ValueRangeConstraint(0, 1))), optional = false, default = None),
      ComponentType("b", Asn1Boolean, optional = true, default = None)
    ))
    val bytes = bits("01000000")
    val row   = unaligned.decodeBytes(bytes, schema)
    row.getLong(0) shouldBe 1L
    row.isNullAt(1) shouldBe true
  }

  // -------------------------------------------------------------------------
  // SEQUENCE OF
  // -------------------------------------------------------------------------

  "PerDecoder (unaligned)" should "decode SEQUENCE OF with unconstrained count" in {
    // Count = 2 (length-det: 1 byte 0x02), elements INTEGER(0..3): 2 bits each
    // In unaligned decoder, length-det calls maybeAlign (no-op in UPER), so:
    // 00000010 (count=2)  10 01 (elem0=2, elem1=1)  000000 padding
    val schema = Asn1SequenceOf(Asn1Integer(Some(ValueRangeConstraint(0, 3))))
    val bytes  = bits("00000010 10010000")
    val row    = unaligned.decodeBytes(bytes, schema)
    val arr    = row.getArray(0)
    arr.numElements() shouldBe 2
    arr.getLong(0)    shouldBe 2L
    arr.getLong(1)    shouldBe 1L
  }

  // -------------------------------------------------------------------------
  // CHOICE
  // -------------------------------------------------------------------------

  "PerDecoder (unaligned)" should "decode CHOICE" in {
    // CHOICE { a INTEGER(0..3), b BOOLEAN }
    // 2 alternatives → 1 index bit; index 0 (a) then value 3 (11)
    // bits: 0 11 00000 = 01100000
    val schema = Asn1Choice(Seq(
      NamedType("a", Asn1Integer(Some(ValueRangeConstraint(0, 3)))),
      NamedType("b", Asn1Boolean)
    ))
    val bytes = bits("01100000")
    val row   = unaligned.decodeBytes(bytes, schema)
    row.getString(0)  shouldBe "a"   // _tag
    row.getLong(1)    shouldBe 3L    // a value
    row.isNullAt(2)   shouldBe true  // b absent
  }

  // -------------------------------------------------------------------------
  // Framing: hex-lines via PerRecordIterator
  // -------------------------------------------------------------------------

  "PerRecordIterator (hex-lines)" should "decode multiple records from hex lines" in {
    import io.github.sparkasn1.spark.asn1.reader.PerRecordIterator
    val schema = Asn1Integer(Some(ValueRangeConstraint(0, 255)))
    // Each line is one byte encoding INTEGER(0..255)
    val input = "41\n42\n43\n".getBytes("UTF-8")
    val stream = new java.io.ByteArrayInputStream(input)
    val iter   = new PerRecordIterator(stream, aligned, schema, "hex-lines")
    iter.next().getLong(0) shouldBe 65L  // 0x41
    iter.next().getLong(0) shouldBe 66L  // 0x42
    iter.next().getLong(0) shouldBe 67L  // 0x43
    iter.hasNext shouldBe false
  }

  "PerRecordIterator (length-prefixed)" should "decode length-prefixed records" in {
    import io.github.sparkasn1.spark.asn1.reader.PerRecordIterator
    val schema = Asn1Integer(Some(ValueRangeConstraint(0, 255)))
    // Two records: [0x00 0x00 0x00 0x01 0x41] [0x00 0x00 0x00 0x01 0x42]
    val data = Array[Byte](0, 0, 0, 1, 0x41.toByte, 0, 0, 0, 1, 0x42.toByte)
    val stream = new java.io.ByteArrayInputStream(data)
    val iter   = new PerRecordIterator(stream, aligned, schema, "length-prefixed")
    iter.next().getLong(0) shouldBe 65L
    iter.next().getLong(0) shouldBe 66L
    iter.hasNext shouldBe false
  }

  "PerRecordIterator (fixed-length)" should "decode fixed-length records" in {
    import io.github.sparkasn1.spark.asn1.reader.PerRecordIterator
    val schema = Asn1Integer(Some(ValueRangeConstraint(0, 255)))
    // Two 1-byte records: 0x41, 0x42
    val data   = Array[Byte](0x41.toByte, 0x42.toByte)
    val stream = new java.io.ByteArrayInputStream(data)
    val iter   = new PerRecordIterator(stream, aligned, schema, "fixed-length", recordBytes = 1)
    iter.next().getLong(0) shouldBe 65L
    iter.next().getLong(0) shouldBe 66L
    iter.hasNext shouldBe false
  }
}
