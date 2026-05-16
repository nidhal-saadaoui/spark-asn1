package io.github.sparkasn1.spark.asn1.schema

import io.github.sparkasn1.spark.asn1.model._
import io.github.sparkasn1.spark.asn1.parser.{Asn1SchemaParser, SchemaRegistry}
import org.apache.spark.sql.types._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class Asn1TypeMapperSpec extends AnyFlatSpec with Matchers {

  private val emptyRegistry = SchemaRegistry(Seq.empty)
  private val mod = "TestMod"

  private def map(t: Asn1Type) =
    Asn1TypeMapper.toSparkType(t, emptyRegistry, mod)

  "Asn1TypeMapper" should "map primitive types" in {
    map(Asn1Boolean)         shouldBe BooleanType
    map(Asn1Null)            shouldBe NullType
    map(Asn1Integer())       shouldBe LongType
    map(Asn1OctetString)     shouldBe BinaryType
    map(Asn1ObjectIdentifier) shouldBe StringType
  }

  it should "map all string types to StringType" in {
    map(Asn1Utf8String)      shouldBe StringType
    map(Asn1PrintableString) shouldBe StringType
    map(Asn1Ia5String)       shouldBe StringType
    map(Asn1VisibleString)   shouldBe StringType
    map(Asn1NumericString)   shouldBe StringType
    map(Asn1GeneralString)   shouldBe StringType
    map(Asn1BmpString)       shouldBe StringType
  }

  it should "map unnamed BIT STRING to BinaryType" in {
    map(Asn1BitString()) shouldBe BinaryType
  }

  it should "map named BIT STRING to struct with _bytes and _namedBits" in {
    val bs = Asn1BitString(Seq(NamedBit("read", 0), NamedBit("write", 1)))
    val st = map(bs).asInstanceOf[StructType]
    st.fields.map(_.name) shouldBe Array("_bytes", "_namedBits")
    st("_bytes").dataType  shouldBe BinaryType
    st("_namedBits").dataType shouldBe ArrayType(StringType)
  }

  it should "map ENUMERATED to StringType" in {
    val e = Asn1Enumerated(Seq(EnumerationItem("a", Some(0))))
    map(e) shouldBe StringType
  }

  it should "map SEQUENCE to StructType" in {
    val seq = Asn1Sequence(Seq(
      ComponentType("id",   Asn1Integer(), optional = false, default = None),
      ComponentType("name", Asn1Utf8String, optional = true, default = None)
    ))
    val st = map(seq).asInstanceOf[StructType]
    st.fields.map(_.name) shouldBe Array("id", "name")
    st("id").nullable   shouldBe false
    st("name").nullable shouldBe true
  }

  it should "map CHOICE to struct with _tag and nullable alternatives" in {
    val choice = Asn1Choice(Seq(
      NamedType("i", Asn1Integer()),
      NamedType("s", Asn1Utf8String)
    ))
    val st = map(choice).asInstanceOf[StructType]
    st.fields.map(_.name) shouldBe Array("_tag", "i", "s")
    st("_tag").nullable shouldBe false
    st("i").nullable    shouldBe true
    st("s").nullable    shouldBe true
    st("i").dataType    shouldBe LongType
    st("s").dataType    shouldBe StringType
  }

  it should "map SEQUENCE OF to ArrayType" in {
    val so = Asn1SequenceOf(Asn1Integer())
    map(so) shouldBe ArrayType(LongType, containsNull = true)
  }

  it should "map nested SEQUENCE" in {
    val inner = Asn1Sequence(Seq(ComponentType("x", Asn1Integer(), optional = false, default = None)))
    val outer = Asn1Sequence(Seq(ComponentType("inner", inner, optional = false, default = None)))
    val st = map(outer).asInstanceOf[StructType]
    st("inner").dataType shouldBe a[StructType]
  }

  it should "map tagged type transparently" in {
    val tt = Asn1TaggedType(TagClass.ContextSpecific, 0, Tagging.Implicit, Asn1Integer())
    map(tt) shouldBe LongType
  }

  it should "map types from a real schema file" in {
    val path = getClass.getResource("/schemas/nested.asn1").getPath
    val m    = Asn1SchemaParser.parseFile(path)
    val reg  = SchemaRegistry(m)
    val nested = m.typeAssignments("NestedRecord").asn1Type
    val sparkType = Asn1TypeMapper.toSparkType(nested, reg, m.name)
    sparkType shouldBe a[StructType]
    val st = sparkType.asInstanceOf[StructType]
    st.fieldNames should contain allOf("id", "status", "permissions", "tags", "labels")
    st("tags").dataType   shouldBe a[ArrayType]
    st("labels").dataType shouldBe ArrayType(StringType, containsNull = true)
  }
}
