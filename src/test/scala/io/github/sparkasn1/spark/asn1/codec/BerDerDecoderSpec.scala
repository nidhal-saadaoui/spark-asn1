package io.github.sparkasn1.spark.asn1.codec

import io.github.sparkasn1.spark.asn1.model._
import io.github.sparkasn1.spark.asn1.parser.SchemaRegistry
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.unsafe.types.UTF8String
import org.bouncycastle.asn1._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.ByteArrayInputStream

class BerDerDecoderSpec extends AnyFlatSpec with Matchers {

  private val registry = SchemaRegistry(Seq.empty)
  private val decoder  = new BerDerDecoder(registry, "TestMod")

  // Helper: DER-encode an ASN1Primitive into bytes
  private def encode(obj: ASN1Primitive): Array[Byte] = obj.getEncoded("DER")

  private def decodeValue(obj: ASN1Primitive, schema: Asn1Type): Any =
    decoder.decodeValue(obj, schema)

  "BerDerDecoder" should "decode BOOLEAN true" in {
    decodeValue(ASN1Boolean.getInstance(true), Asn1Boolean)  shouldBe true
    decodeValue(ASN1Boolean.getInstance(false), Asn1Boolean) shouldBe false
  }

  it should "decode INTEGER" in {
    decodeValue(new ASN1Integer(42), Asn1Integer()) shouldBe 42L
    decodeValue(new ASN1Integer(-1), Asn1Integer()) shouldBe -1L
  }

  it should "decode OCTET STRING" in {
    val bytes = Array[Byte](1, 2, 3)
    val result = decodeValue(new DEROctetString(bytes), Asn1OctetString)
    result shouldBe bytes
  }

  it should "decode OBJECT IDENTIFIER" in {
    val oid = new ASN1ObjectIdentifier("1.2.840.10045.2.1")
    decodeValue(oid, Asn1ObjectIdentifier) shouldBe UTF8String.fromString("1.2.840.10045.2.1")
  }

  it should "decode UTF8String" in {
    val s = new DERUTF8String("hello")
    decodeValue(s, Asn1Utf8String) shouldBe UTF8String.fromString("hello")
  }

  it should "decode ENUMERATED by name" in {
    val schema = Asn1Enumerated(Seq(
      EnumerationItem("red",  Some(0)),
      EnumerationItem("green", Some(1)),
      EnumerationItem("blue",  Some(2))
    ))
    decodeValue(new ASN1Enumerated(1), schema) shouldBe UTF8String.fromString("green")
  }

  it should "decode unnamed BIT STRING as bytes" in {
    val bs     = new DERBitString(Array[Byte](0xff.toByte, 0x80.toByte))
    val result = decodeValue(bs, Asn1BitString())
    result shouldBe a[Array[_]]
  }

  it should "decode named BIT STRING as struct" in {
    val schema = Asn1BitString(Seq(
      NamedBit("read",    0),
      NamedBit("write",   1),
      NamedBit("execute", 2)
    ))
    // bit 0 (read) and bit 2 (execute) set → 0b10100000 = 0xA0
    val bs     = new DERBitString(Array[Byte](0xa0.toByte), 5)
    val result = decodeValue(bs, schema).asInstanceOf[GenericInternalRow]
    result should not be null
    // _bytes at index 0, _namedBits at index 1
  }

  it should "decode SEQUENCE" in {
    val schema = Asn1Sequence(Seq(
      ComponentType("id",   Asn1Integer(),  optional = false, default = None),
      ComponentType("name", Asn1Utf8String, optional = false, default = None)
    ))
    val seq = new DERSequence(Array[ASN1Encodable](
      new ASN1Integer(99),
      new DERUTF8String("test")
    ))
    val row = decoder.decodeBytes(encode(seq), schema)
    row.getLong(0)                shouldBe 99L
    row.getUTF8String(1).toString shouldBe "test"
  }

  it should "decode SEQUENCE OF" in {
    val schema = Asn1SequenceOf(Asn1Integer())
    val seq = new DERSequence(Array[ASN1Encodable](
      new ASN1Integer(1),
      new ASN1Integer(2),
      new ASN1Integer(3)
    ))
    val row = decoder.decodeBytes(encode(seq), schema)
    // single-value wrapper row
    row should not be null
  }

  it should "decode multiple records from a stream" in {
    val schema = Asn1Sequence(Seq(
      ComponentType("id", Asn1Integer(), optional = false, default = None)
    ))
    val rec1 = encode(new DERSequence(Array[ASN1Encodable](new ASN1Integer(1))))
    val rec2 = encode(new DERSequence(Array[ASN1Encodable](new ASN1Integer(2))))
    val allBytes = rec1 ++ rec2

    val is     = new ByteArrayInputStream(allBytes)
    val parser = decoder.openParser(is)
    val row1   = decoder.decodeNext(parser, schema)
    val row2   = decoder.decodeNext(parser, schema)
    val row3   = decoder.decodeNext(parser, schema)

    row1.map(_.getLong(0)) shouldBe Some(1L)
    row2.map(_.getLong(0)) shouldBe Some(2L)
    row3                   shouldBe None
  }

  it should "return None when stream is empty" in {
    val is     = new ByteArrayInputStream(Array.emptyByteArray)
    val parser = decoder.openParser(is)
    decoder.decodeNext(parser, Asn1Boolean) shouldBe None
  }
}
