package io.github.sparkasn1.spark.asn1.schema

import io.github.sparkasn1.spark.asn1.model._
import io.github.sparkasn1.spark.asn1.parser.SchemaRegistry
import org.apache.spark.sql.types._

/**
 * Maps a resolved Asn1Type to a Spark SQL DataType.
 *
 * CHOICE becomes a StructType with a `_tag` StringType discriminator field and one
 * nullable field per alternative.
 *
 * NAMED BIT STRING becomes StructType(_bytes: BinaryType, _namedBits: ArrayType(StringType)).
 */
object Asn1TypeMapper {

  val ChoiceTagField = "_tag"
  val BitStringBytesField = "_bytes"
  val BitStringNamesField = "_namedBits"

  /**
   * Convert an ASN.1 type to a Spark DataType.
   * @param t              the (fully resolved) ASN.1 type
   * @param registry       schema registry for resolving any remaining references
   * @param currentModule  module name used when resolving unqualified references
   * @param choiceTagField field name used for the CHOICE discriminator (override via option)
   */
  def toSparkType(
    t: Asn1Type,
    registry: SchemaRegistry,
    currentModule: String,
    choiceTagField: String  = ChoiceTagField,
    enumeratedAsInt: Boolean = false
  ): DataType = convert(t, registry, currentModule, choiceTagField, enumeratedAsInt)

  private def convert(
    t: Asn1Type,
    reg: SchemaRegistry,
    mod: String,
    tagField: String,
    enumAsInt: Boolean = false
  ): DataType = t match {

    case Asn1Boolean         => BooleanType
    case Asn1Null            => NullType
    case _: Asn1Integer      => LongType
    case Asn1OctetString     => BinaryType
    case Asn1ObjectIdentifier => StringType
    case Asn1RelativeOid     => StringType
    case Asn1Any             => BinaryType
    case Asn1Real            => DoubleType

    // All restricted character string types → StringType
    case _: Asn1StringType   => StringType

    case b: Asn1BitString =>
      if (b.namedBits.isEmpty)
        BinaryType
      else
        StructType(Seq(
          StructField(BitStringBytesField, BinaryType,            nullable = false),
          StructField(BitStringNamesField, ArrayType(StringType), nullable = false)
        ))

    case _: Asn1Enumerated   => if (enumAsInt) LongType else StringType

    case s: Asn1Sequence =>
      StructType(s.components.map(c => componentToField(c, reg, mod, tagField, enumAsInt)))

    case s: Asn1Set =>
      StructType(s.components.map(c => componentToField(c, reg, mod, tagField, enumAsInt)))

    case s: Asn1SequenceOf =>
      val elemType = convert(s.elementType, reg, mod, tagField, enumAsInt)
      ArrayType(elemType, containsNull = true)

    case s: Asn1SetOf =>
      val elemType = convert(s.elementType, reg, mod, tagField, enumAsInt)
      ArrayType(elemType, containsNull = true)

    case c: Asn1Choice =>
      val tagField_ = StructField(tagField, StringType, nullable = false)
      val altFields = c.alternatives.map { alt =>
        val altType = convert(alt.asn1Type, reg, mod, tagField, enumAsInt)
        StructField(alt.name, altType, nullable = true)
      }
      StructType(tagField_ +: altFields)

    case tt: Asn1TaggedType =>
      convert(tt.innerType, reg, mod, tagField, enumAsInt)

    case ref: Asn1TypeReference =>
      val resolved = reg.resolveRef(ref, mod)
      if (resolved == ref) BinaryType
      else convert(resolved, reg, mod, tagField, enumAsInt)
  }

  private def componentToField(
    c: ComponentType,
    reg: SchemaRegistry,
    mod: String,
    tagField: String,
    enumAsInt: Boolean = false
  ): StructField = {
    val sparkType = convert(c.asn1Type, reg, mod, tagField, enumAsInt)
    val nullable  = c.optional || c.default.isDefined
    StructField(c.name, sparkType, nullable = nullable)
  }
}
