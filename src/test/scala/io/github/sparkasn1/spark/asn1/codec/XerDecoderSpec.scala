package io.github.sparkasn1.spark.asn1.codec

import io.github.sparkasn1.spark.asn1.codec.xer.XerDecoder
import io.github.sparkasn1.spark.asn1.model._
import io.github.sparkasn1.spark.asn1.parser.SchemaRegistry
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class XerDecoderSpec extends AnyFlatSpec with Matchers {

  private val registry = SchemaRegistry(Seq.empty)
  private def decoder  = new XerDecoder(registry, "Test")

  // -------------------------------------------------------------------------
  // Primitives
  // -------------------------------------------------------------------------

  "XerDecoder" should "decode BOOLEAN <true/>" in {
    val row = decoder.decodeXml("<active><true/></active>", Asn1Boolean)
    row.getBoolean(0) shouldBe true
  }

  it should "decode BOOLEAN <false/>" in {
    val row = decoder.decodeXml("<active><false/></active>", Asn1Boolean)
    row.getBoolean(0) shouldBe false
  }

  it should "decode NULL as null" in {
    val row = decoder.decodeXml("<val></val>", Asn1Null)
    row.isNullAt(0) shouldBe true
  }

  it should "decode INTEGER from text content" in {
    val row = decoder.decodeXml("<id>42</id>", Asn1Integer())
    row.getLong(0) shouldBe 42L
  }

  it should "decode negative INTEGER" in {
    val row = decoder.decodeXml("<id>-17</id>", Asn1Integer())
    row.getLong(0) shouldBe -17L
  }

  it should "decode OCTET STRING from hex text" in {
    val row = decoder.decodeXml("<data>0102FF</data>", Asn1OctetString)
    row.getBinary(0) shouldBe Array[Byte](1, 2, -1)
  }

  it should "decode OBJECT IDENTIFIER" in {
    val row = decoder.decodeXml("<oid>1.2.840.10045.2.1</oid>", Asn1ObjectIdentifier)
    row.getString(0) shouldBe "1.2.840.10045.2.1"
  }

  it should "decode UTF8String" in {
    val row = decoder.decodeXml("<name>hello world</name>", Asn1Utf8String)
    row.getString(0) shouldBe "hello world"
  }

  it should "decode ENUMERATED via inner element name" in {
    val schema = Asn1Enumerated(Seq(
      EnumerationItem("active",   Some(0)),
      EnumerationItem("inactive", Some(1)),
      EnumerationItem("pending",  Some(2))
    ))
    val row = decoder.decodeXml("<status><inactive/></status>", schema)
    row.getString(0) shouldBe "inactive"
  }

  it should "decode unnamed BIT STRING from hex" in {
    val row = decoder.decodeXml("<flags>A0</flags>", Asn1BitString())
    row.getBinary(0) shouldBe Array[Byte](0xa0.toByte)
  }

  it should "decode named BIT STRING as struct" in {
    val schema = Asn1BitString(namedBits = Seq(
      NamedBit("read",    0),
      NamedBit("write",   1),
      NamedBit("execute", 2)
    ))
    // A0 = 10100000 → bits 0 (read) and 2 (execute) are set
    // Named BIT STRING is the root struct: field 0 = _bytes, field 1 = _namedBits
    val row = decoder.decodeXml("<perms>A0</perms>", schema)
    row.getBinary(0) shouldBe Array[Byte](0xa0.toByte)
    val names = row.getArray(1)
    names.numElements() shouldBe 2
    names.getUTF8String(0).toString shouldBe "read"
    names.getUTF8String(1).toString shouldBe "execute"
  }

  // -------------------------------------------------------------------------
  // SEQUENCE
  // -------------------------------------------------------------------------

  it should "decode a SEQUENCE" in {
    val schema = Asn1Sequence(Seq(
      ComponentType("id",     Asn1Integer(),   optional = false, default = None),
      ComponentType("name",   Asn1Utf8String,  optional = false, default = None),
      ComponentType("active", Asn1Boolean,     optional = false, default = None)
    ))
    val xml =
      """<Rec>
        |  <id>7</id>
        |  <name>Alice</name>
        |  <active><true/></active>
        |</Rec>""".stripMargin
    val row = decoder.decodeXml(xml, schema)
    row.getLong(0)    shouldBe 7L
    row.getString(1)  shouldBe "Alice"
    row.getBoolean(2) shouldBe true
  }

  it should "decode a SEQUENCE with absent OPTIONAL field" in {
    val schema = Asn1Sequence(Seq(
      ComponentType("id",    Asn1Integer(),  optional = false, default = None),
      ComponentType("alias", Asn1Utf8String, optional = true,  default = None)
    ))
    val xml = "<Rec><id>3</id></Rec>"
    val row = decoder.decodeXml(xml, schema)
    row.getLong(0)  shouldBe 3L
    row.isNullAt(1) shouldBe true
  }

  // -------------------------------------------------------------------------
  // SEQUENCE OF
  // -------------------------------------------------------------------------

  it should "decode SEQUENCE OF" in {
    val schema = Asn1SequenceOf(Asn1Integer())
    val xml    = "<nums><item>1</item><item>2</item><item>3</item></nums>"
    val row    = decoder.decodeXml(xml, schema)
    val arr    = row.getArray(0)
    arr.numElements() shouldBe 3
    arr.getLong(0) shouldBe 1L
    arr.getLong(1) shouldBe 2L
    arr.getLong(2) shouldBe 3L
  }

  // -------------------------------------------------------------------------
  // CHOICE
  // -------------------------------------------------------------------------

  it should "decode CHOICE via child element name" in {
    val schema = Asn1Choice(Seq(
      NamedType("intVal",  Asn1Integer()),
      NamedType("strVal",  Asn1Utf8String)
    ))
    val xml = "<val><strVal>hello</strVal></val>"
    val row = decoder.decodeXml(xml, schema)
    row.getString(0)  shouldBe "strVal"   // _tag
    row.isNullAt(1)   shouldBe true       // intVal absent
    row.getString(2)  shouldBe "hello"    // strVal present
  }

  // -------------------------------------------------------------------------
  // XerRecordIterator
  // -------------------------------------------------------------------------

  "XerRecordIterator" should "split and decode multiple top-level elements" in {
    import io.github.sparkasn1.spark.asn1.reader.XerRecordIterator
    val schema = Asn1Integer()
    val xml    = "<id>10</id><id>20</id><id>30</id>"
    val stream = new java.io.ByteArrayInputStream(xml.getBytes("UTF-8"))
    val iter   = new XerRecordIterator(stream, decoder, schema)
    iter.next().getLong(0) shouldBe 10L
    iter.next().getLong(0) shouldBe 20L
    iter.next().getLong(0) shouldBe 30L
    iter.hasNext shouldBe false
  }

  it should "handle a wrapper element" in {
    import io.github.sparkasn1.spark.asn1.reader.XerRecordIterator
    val schema = Asn1Integer()
    val xml    = "<Records><id>5</id><id>6</id></Records>"
    val stream = new java.io.ByteArrayInputStream(xml.getBytes("UTF-8"))
    val iter   = new XerRecordIterator(stream, decoder, schema)
    // At least the first record should decode
    iter.hasNext shouldBe true
    iter.next().getLong(0) shouldBe 5L
  }
}
