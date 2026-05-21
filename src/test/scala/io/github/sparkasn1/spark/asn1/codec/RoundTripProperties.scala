package io.github.sparkasn1.spark.asn1.codec

import io.github.sparkasn1.spark.asn1.codec.per.{AlignedPerDecoder, AlignedPerEncoder, UnalignedPerDecoder, UnalignedPerEncoder}
import io.github.sparkasn1.spark.asn1.codec.xer.{XerDecoder, XerEncoder}
import io.github.sparkasn1.spark.asn1.model.Asn1Type
import io.github.sparkasn1.spark.asn1.parser.SchemaRegistry
import io.github.sparkasn1.spark.asn1.schema.Asn1TypeMapper
import io.github.sparkasn1.spark.asn1.util.SchemaCache
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.scalacheck.Gen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import java.io.File
import java.nio.charset.StandardCharsets

/**
 * Property-based round-trip tests: encode → decode must recover the original row.
 *
 * Uses ScalaCheck generators to produce random InternalRow values for each test
 * schema; runs 200 iterations per property by default.  No SparkSession required
 * — all types come from Spark's internal API, which is available on the classpath.
 */
class RoundTripProperties extends AnyFlatSpec with Matchers with ScalaCheckPropertyChecks {

  implicit override val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(minSuccessful = 200)

  // --------------------------------------------------------------------------
  // Schemas
  // --------------------------------------------------------------------------

  private val schemaDir = new File(getClass.getResource("/schemas").getPath)

  private val simpleReg = SchemaCache.getOrParse(Seq(new File(schemaDir, "simple.asn1").getAbsolutePath))
  private val nestedReg = SchemaCache.getOrParse(Seq(new File(schemaDir, "nested.asn1").getAbsolutePath))
  private val choiceReg = SchemaCache.getOrParse(Seq(new File(schemaDir, "choice.asn1").getAbsolutePath))

  private val simpleAsn1  = simpleReg.resolve("SimpleRecord", "SimpleModule").get
  private val personAsn1  = simpleReg.resolve("PersonRecord", "SimpleModule").get
  private val nestedAsn1  = nestedReg.resolve("NestedRecord", "NestedModule").get
  private val wrapperAsn1 = choiceReg.resolve("Wrapper",      "ChoiceModule").get

  private val simpleSchema  = Asn1TypeMapper.toSparkType(simpleAsn1,  simpleReg, "SimpleModule").asInstanceOf[StructType]
  private val personSchema  = Asn1TypeMapper.toSparkType(personAsn1,  simpleReg, "SimpleModule").asInstanceOf[StructType]
  private val nestedSchema  = Asn1TypeMapper.toSparkType(nestedAsn1,  nestedReg, "NestedModule").asInstanceOf[StructType]
  private val wrapperSchema = Asn1TypeMapper.toSparkType(wrapperAsn1, choiceReg, "ChoiceModule").asInstanceOf[StructType]

  // --------------------------------------------------------------------------
  // Generators
  // --------------------------------------------------------------------------

  private val validOids = Seq("1.2.3", "1.2.840.113549.1.1.11", "2.5.4.3", "1.3.6.1.4.1.1234")

  // SimpleRecord: id INTEGER, name UTF8String, active BOOLEAN,
  //               score INTEGER(0..100), data OCTET STRING, oid OBJECT IDENTIFIER
  private val genSimpleRow: Gen[InternalRow] = for {
    id     <- Gen.choose(-1000000L, 1000000L)
    name   <- Gen.alphaNumStr.suchThat(_.nonEmpty)
    active <- Gen.oneOf(true, false)
    score  <- Gen.choose(0L, 100L)
    data   <- Gen.listOf(Gen.choose(0, 255).map(_.toByte)).map(_.toArray)
    oid    <- Gen.oneOf(validOids)
  } yield new GenericInternalRow(Array[Any](
    id, UTF8String.fromString(name), active, score, data, UTF8String.fromString(oid)
  ))

  // PersonRecord: firstName UTF8String, lastName UTF8String,
  //               age INTEGER OPTIONAL, email UTF8String OPTIONAL
  private val genPersonRow: Gen[InternalRow] = for {
    firstName <- Gen.alphaNumStr.suchThat(_.nonEmpty)
    lastName  <- Gen.alphaNumStr.suchThat(_.nonEmpty)
    age       <- Gen.option(Gen.choose(0L, 150L))
    email     <- Gen.option(Gen.alphaNumStr)
  } yield new GenericInternalRow(Array[Any](
    UTF8String.fromString(firstName),
    UTF8String.fromString(lastName),
    age.orNull,
    email.map(UTF8String.fromString).orNull
  ))

  // Permissions named BIT STRING: read(0), write(1), execute(2) — ASN.1 bit 0 = MSB of byte.
  // Generate one Boolean per bit; _bytes and _namedBits must be consistent for a clean round-trip.
  private val genPermissionsRow: Gen[InternalRow] = for {
    read    <- Gen.oneOf(true, false)
    write   <- Gen.oneOf(true, false)
    execute <- Gen.oneOf(true, false)
  } yield {
    val byteVal   = (if (read) 0x80 else 0) | (if (write) 0x40 else 0) | (if (execute) 0x20 else 0)
    val namedBits = Seq("read" -> read, "write" -> write, "execute" -> execute)
                      .collect { case (name, true) => UTF8String.fromString(name) }
    new GenericInternalRow(Array[Any](
      Array(byteVal.toByte),
      new GenericArrayData(namedBits.toArray[Any])
    ))
  }

  private val genTagRow: Gen[InternalRow] = for {
    key   <- Gen.alphaNumStr.suchThat(_.nonEmpty)
    value <- Gen.alphaNumStr
  } yield new GenericInternalRow(Array[Any](
    UTF8String.fromString(key), UTF8String.fromString(value)
  ))

  // NestedRecord: id INTEGER, status ENUMERATED, permissions Permissions,
  //               tags SEQUENCE OF Tag, labels SEQUENCE OF UTF8String
  private val genNestedRow: Gen[InternalRow] = for {
    id      <- Gen.choose(-1000000L, 1000000L)
    status  <- Gen.oneOf("active", "inactive", "pending")
    perms   <- genPermissionsRow
    tags    <- Gen.listOf(genTagRow).map(lst => new GenericArrayData(lst.toArray[Any]))
    labels  <- Gen.listOf(Gen.alphaNumStr).map { lst =>
                 new GenericArrayData(lst.map(UTF8String.fromString).toArray[Any])
               }
  } yield new GenericInternalRow(Array[Any](
    id, UTF8String.fromString(status), perms, tags, labels
  ))

  // Value ::= CHOICE { intVal INTEGER, strVal UTF8String, boolVal BOOLEAN }
  // Spark: StructType(_tag, intVal nullable, strVal nullable, boolVal nullable)
  private val genValueRow: Gen[InternalRow] = Gen.oneOf(
    Gen.choose(-1000000L, 1000000L).map { n =>
      new GenericInternalRow(Array[Any](UTF8String.fromString("intVal"), n, null, null))
    },
    Gen.alphaNumStr.map { s =>
      new GenericInternalRow(Array[Any](UTF8String.fromString("strVal"), null, UTF8String.fromString(s), null))
    },
    Gen.oneOf(true, false).map { b =>
      new GenericInternalRow(Array[Any](UTF8String.fromString("boolVal"), null, null, b))
    }
  )

  // Wrapper: id INTEGER, value Value(CHOICE)
  private val genWrapperRow: Gen[InternalRow] = for {
    id    <- Gen.choose(-1000000L, 1000000L)
    value <- genValueRow
  } yield new GenericInternalRow(Array[Any](id, value))

  // --------------------------------------------------------------------------
  // Structural equality (handles UTF8String, Array[Byte], ArrayData, InternalRow)
  // --------------------------------------------------------------------------

  private def normalize(value: Any, dt: DataType): Any = (value, dt) match {
    case (null, _)                                    => null
    case (s: UTF8String, _)                           => s.toString
    case (arr: Array[Byte], BinaryType)               => arr.toList
    case (arr: ArrayData, ArrayType(elemType, _))     =>
      (0 until arr.numElements()).map(i => normalize(arr.get(i, elemType), elemType)).toList
    case (row: InternalRow, st: StructType)           =>
      st.fields.zipWithIndex.map { case (f, i) => normalize(row.get(i, f.dataType), f.dataType) }.toList
    case _                                            => value
  }

  private def assertRoundTrip(
    original: InternalRow,
    decoded:  InternalRow,
    schema:   StructType
  ): Unit =
    normalize(decoded, schema) shouldBe normalize(original, schema)

  // --------------------------------------------------------------------------
  // Round-trip helpers
  // --------------------------------------------------------------------------

  private def berRoundTrip(reg: SchemaRegistry, mod: String)(
    row: InternalRow, schema: StructType, asn1: Asn1Type
  ): Unit = {
    val bytes   = new BerDerEncoder(reg, mod).encodeRow(row, schema, asn1)
    val decoded = new BerDerDecoder(reg, mod).decodeBytes(bytes, asn1)
    assertRoundTrip(row, decoded, schema)
  }

  private def aperRoundTrip(reg: SchemaRegistry, mod: String)(
    row: InternalRow, schema: StructType, asn1: Asn1Type
  ): Unit = {
    val bytes   = new AlignedPerEncoder(reg, mod).encodeRow(row, schema, asn1)
    val decoded = new AlignedPerDecoder(reg, mod).decodeBytes(bytes, asn1)
    assertRoundTrip(row, decoded, schema)
  }

  private def uperRoundTrip(reg: SchemaRegistry, mod: String)(
    row: InternalRow, schema: StructType, asn1: Asn1Type
  ): Unit = {
    val bytes   = new UnalignedPerEncoder(reg, mod).encodeRow(row, schema, asn1)
    val decoded = new UnalignedPerDecoder(reg, mod).decodeBytes(bytes, asn1)
    assertRoundTrip(row, decoded, schema)
  }

  private def xerRoundTrip(reg: SchemaRegistry, mod: String, rootTag: String)(
    row: InternalRow, schema: StructType, asn1: Asn1Type
  ): Unit = {
    val bytes   = new XerEncoder(reg, mod).encodeRow(row, schema, asn1, rootTag)
    val xml     = new String(bytes, StandardCharsets.UTF_8).trim
    val decoded = new XerDecoder(reg, mod).decodeXml(xml, asn1)
    assertRoundTrip(row, decoded, schema)
  }

  // --------------------------------------------------------------------------
  // BER
  // --------------------------------------------------------------------------

  "BER round-trip" should "hold for SimpleRecord" in {
    forAll(genSimpleRow) { row =>
      berRoundTrip(simpleReg, "SimpleModule")(row, simpleSchema, simpleAsn1)
    }
  }

  it should "hold for PersonRecord with OPTIONAL fields" in {
    forAll(genPersonRow) { row =>
      berRoundTrip(simpleReg, "SimpleModule")(row, personSchema, personAsn1)
    }
  }

  it should "hold for NestedRecord (SEQUENCE OF, ENUMERATED, named BIT STRING)" in {
    forAll(genNestedRow) { row =>
      berRoundTrip(nestedReg, "NestedModule")(row, nestedSchema, nestedAsn1)
    }
  }

  it should "hold for Wrapper (CHOICE)" in {
    forAll(genWrapperRow) { row =>
      berRoundTrip(choiceReg, "ChoiceModule")(row, wrapperSchema, wrapperAsn1)
    }
  }

  // --------------------------------------------------------------------------
  // APER
  // --------------------------------------------------------------------------

  "APER round-trip" should "hold for SimpleRecord" in {
    forAll(genSimpleRow) { row =>
      aperRoundTrip(simpleReg, "SimpleModule")(row, simpleSchema, simpleAsn1)
    }
  }

  it should "hold for PersonRecord with OPTIONAL fields" in {
    forAll(genPersonRow) { row =>
      aperRoundTrip(simpleReg, "SimpleModule")(row, personSchema, personAsn1)
    }
  }

  it should "hold for Wrapper (CHOICE)" in {
    forAll(genWrapperRow) { row =>
      aperRoundTrip(choiceReg, "ChoiceModule")(row, wrapperSchema, wrapperAsn1)
    }
  }

  // --------------------------------------------------------------------------
  // UPER
  // --------------------------------------------------------------------------

  "UPER round-trip" should "hold for SimpleRecord" in {
    forAll(genSimpleRow) { row =>
      uperRoundTrip(simpleReg, "SimpleModule")(row, simpleSchema, simpleAsn1)
    }
  }

  it should "hold for PersonRecord with OPTIONAL fields" in {
    forAll(genPersonRow) { row =>
      uperRoundTrip(simpleReg, "SimpleModule")(row, personSchema, personAsn1)
    }
  }

  it should "hold for Wrapper (CHOICE)" in {
    forAll(genWrapperRow) { row =>
      uperRoundTrip(choiceReg, "ChoiceModule")(row, wrapperSchema, wrapperAsn1)
    }
  }

  // --------------------------------------------------------------------------
  // XER
  // --------------------------------------------------------------------------

  "XER round-trip" should "hold for SimpleRecord" in {
    forAll(genSimpleRow) { row =>
      xerRoundTrip(simpleReg, "SimpleModule", "SimpleRecord")(row, simpleSchema, simpleAsn1)
    }
  }

  it should "hold for PersonRecord with OPTIONAL fields" in {
    forAll(genPersonRow) { row =>
      xerRoundTrip(simpleReg, "SimpleModule", "PersonRecord")(row, personSchema, personAsn1)
    }
  }

  it should "hold for NestedRecord (SEQUENCE OF, ENUMERATED, named BIT STRING)" in {
    forAll(genNestedRow) { row =>
      xerRoundTrip(nestedReg, "NestedModule", "NestedRecord")(row, nestedSchema, nestedAsn1)
    }
  }

  it should "hold for Wrapper (CHOICE)" in {
    forAll(genWrapperRow) { row =>
      xerRoundTrip(choiceReg, "ChoiceModule", "Wrapper")(row, wrapperSchema, wrapperAsn1)
    }
  }
}
