package io.github.sparkasn1.spark.asn1.parser

import io.github.sparkasn1.spark.asn1.model._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class Asn1SchemaParserSpec extends AnyFlatSpec with Matchers {

  private def parse(asn1: String) = Asn1SchemaParser.parseString(asn1)

  "Asn1SchemaParser" should "parse a simple SEQUENCE" in {
    val m = parse("""
      TestModule DEFINITIONS AUTOMATIC TAGS ::= BEGIN
        MyRec ::= SEQUENCE {
          id   INTEGER,
          name UTF8String
        }
      END
    """)
    m.name shouldBe "TestModule"
    m.tagDefault shouldBe TagDefault.Automatic
    val ta = m.typeAssignments("MyRec")
    ta.asn1Type shouldBe a[Asn1Sequence]
    val seq = ta.asn1Type.asInstanceOf[Asn1Sequence]
    seq.components should have size 2
    seq.components(0).name shouldBe "id"
    seq.components(0).asn1Type shouldBe Asn1Integer()
    seq.components(1).name shouldBe "name"
    seq.components(1).asn1Type shouldBe Asn1Utf8String
  }

  it should "parse OPTIONAL and DEFAULT components" in {
    val m = parse("""
      Mod DEFINITIONS ::= BEGIN
        R ::= SEQUENCE {
          a INTEGER,
          b UTF8String OPTIONAL,
          c INTEGER DEFAULT 0
        }
      END
    """)
    val seq = m.typeAssignments("R").asn1Type.asInstanceOf[Asn1Sequence]
    seq.components(0).optional shouldBe false
    seq.components(1).optional shouldBe true
    seq.components(2).optional shouldBe true
    seq.components(2).default  shouldBe Some("0")
  }

  it should "parse CHOICE type" in {
    val m = parse("""
      Mod DEFINITIONS AUTOMATIC TAGS ::= BEGIN
        V ::= CHOICE {
          i INTEGER,
          s UTF8String
        }
      END
    """)
    val c = m.typeAssignments("V").asn1Type.asInstanceOf[Asn1Choice]
    c.alternatives should have size 2
    c.alternatives(0).name shouldBe "i"
    c.alternatives(1).name shouldBe "s"
  }

  it should "parse ENUMERATED type" in {
    val m = parse("""
      Mod DEFINITIONS ::= BEGIN
        Color ::= ENUMERATED { red(0), green(1), blue(2) }
      END
    """)
    val e = m.typeAssignments("Color").asn1Type.asInstanceOf[Asn1Enumerated]
    e.values.map(_.name) shouldBe Seq("red", "green", "blue")
    e.values.map(_.value) shouldBe Seq(Some(0L), Some(1L), Some(2L))
  }

  it should "parse SEQUENCE OF" in {
    val m = parse("""
      Mod DEFINITIONS ::= BEGIN
        List ::= SEQUENCE OF INTEGER
      END
    """)
    val so = m.typeAssignments("List").asn1Type.asInstanceOf[Asn1SequenceOf]
    so.elementType shouldBe Asn1Integer()
  }

  it should "parse SEQUENCE OF with SIZE constraint" in {
    val m = parse("""
      Mod DEFINITIONS ::= BEGIN
        BoundedList ::= SEQUENCE (SIZE(1..10)) OF INTEGER
      END
    """)
    val so = m.typeAssignments("BoundedList").asn1Type.asInstanceOf[Asn1SequenceOf]
    so.sizeConstraint shouldBe Some(SizeConstraint(1, 10))
  }

  it should "parse BIT STRING with named bits" in {
    val m = parse("""
      Mod DEFINITIONS ::= BEGIN
        Flags ::= BIT STRING { a(0), b(1), c(2) }
      END
    """)
    val bs = m.typeAssignments("Flags").asn1Type.asInstanceOf[Asn1BitString]
    bs.namedBits.map(_.name) shouldBe Seq("a", "b", "c")
    bs.namedBits.map(_.index) shouldBe Seq(0, 1, 2)
  }

  it should "parse INTEGER with value-range constraint" in {
    val m = parse("""
      Mod DEFINITIONS ::= BEGIN
        Port ::= INTEGER (0..65535)
      END
    """)
    val i = m.typeAssignments("Port").asn1Type.asInstanceOf[Asn1Integer]
    i.constraint shouldBe Some(ValueRangeConstraint(0, 65535))
  }

  it should "parse all restricted character string types" in {
    val m = parse("""
      Mod DEFINITIONS ::= BEGIN
        A ::= UTF8String
        B ::= PrintableString
        C ::= IA5String
        D ::= VisibleString
        E ::= NumericString
        F ::= GeneralString
        G ::= BMPString
      END
    """)
    m.typeAssignments("A").asn1Type shouldBe Asn1Utf8String
    m.typeAssignments("B").asn1Type shouldBe Asn1PrintableString
    m.typeAssignments("C").asn1Type shouldBe Asn1Ia5String
    m.typeAssignments("D").asn1Type shouldBe Asn1VisibleString
    m.typeAssignments("E").asn1Type shouldBe Asn1NumericString
    m.typeAssignments("F").asn1Type shouldBe Asn1GeneralString
    m.typeAssignments("G").asn1Type shouldBe Asn1BmpString
  }

  it should "parse tagged type" in {
    val m = parse("""
      Mod DEFINITIONS ::= BEGIN
        T ::= [0] IMPLICIT INTEGER
      END
    """)
    val tt = m.typeAssignments("T").asn1Type.asInstanceOf[Asn1TaggedType]
    tt.tagNumber  shouldBe 0
    tt.tagClass   shouldBe TagClass.ContextSpecific
    tt.tagging    shouldBe Tagging.Implicit
    tt.innerType  shouldBe Asn1Integer()
  }

  it should "parse IMPORTS" in {
    val m = parse("""
      ModB DEFINITIONS ::= BEGIN
        IMPORTS MyType FROM ModA;
        R ::= SEQUENCE { x MyType }
      END
    """)
    m.imports should have size 1
    m.imports(0).symbols    shouldBe Seq("MyType")
    m.imports(0).fromModule shouldBe "ModA"
  }

  it should "parse the simple.asn1 fixture" in {
    val path = getClass.getResource("/schemas/simple.asn1").getPath
    val m    = Asn1SchemaParser.parseFile(path)
    m.name shouldBe "SimpleModule"
    m.typeAssignments.keys should contain allOf("SimpleRecord", "PersonRecord")
  }

  it should "parse the nested.asn1 fixture" in {
    val path = getClass.getResource("/schemas/nested.asn1").getPath
    val m    = Asn1SchemaParser.parseFile(path)
    m.typeAssignments.keys should contain allOf("NestedRecord", "Status", "Permissions", "Tag")
  }
}
