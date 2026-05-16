package io.github.sparkasn1.spark.asn1.model

/** Sealed ADT representing the ASN.1 type system. */
sealed trait Asn1Type

// Primitive types -------------------------------------------------------

case object Asn1Boolean         extends Asn1Type
case object Asn1Null            extends Asn1Type
case object Asn1OctetString     extends Asn1Type
case object Asn1ObjectIdentifier extends Asn1Type
case object Asn1RelativeOid     extends Asn1Type
case object Asn1Any             extends Asn1Type

case class Asn1Integer(constraint: Option[ValueRangeConstraint] = None) extends Asn1Type

case class Asn1BitString(
  namedBits: Seq[NamedBit]             = Seq.empty,
  sizeConstraint: Option[SizeConstraint] = None
) extends Asn1Type

// Restricted character string types — all map to StringType -------------

sealed trait Asn1StringType extends Asn1Type
case object Asn1Utf8String        extends Asn1StringType
case object Asn1NumericString     extends Asn1StringType
case object Asn1PrintableString   extends Asn1StringType
case object Asn1TeletexString     extends Asn1StringType
case object Asn1VideotexString    extends Asn1StringType
case object Asn1Ia5String         extends Asn1StringType
case object Asn1GraphicString     extends Asn1StringType
case object Asn1VisibleString     extends Asn1StringType
case object Asn1GeneralString     extends Asn1StringType
case object Asn1UniversalString   extends Asn1StringType
case object Asn1BmpString         extends Asn1StringType
case object Asn1GeneralizedTime   extends Asn1StringType
case object Asn1UtcTime           extends Asn1StringType

// Enumerated ------------------------------------------------------------

case class Asn1Enumerated(values: Seq[EnumerationItem]) extends Asn1Type

// Constructed types -----------------------------------------------------

case class Asn1Sequence(
  components: Seq[ComponentType],
  extensible: Boolean = false
) extends Asn1Type

case class Asn1SequenceOf(
  elementType: Asn1Type,
  sizeConstraint: Option[SizeConstraint] = None
) extends Asn1Type

case class Asn1Set(
  components: Seq[ComponentType],
  extensible: Boolean = false
) extends Asn1Type

case class Asn1SetOf(
  elementType: Asn1Type,
  sizeConstraint: Option[SizeConstraint] = None
) extends Asn1Type

case class Asn1Choice(
  alternatives: Seq[NamedType],
  extensible: Boolean = false
) extends Asn1Type

/** Tagged type: [tagClass tagNumber] IMPLICIT/EXPLICIT innerType. */
case class Asn1TaggedType(
  tagClass: TagClass,
  tagNumber: Int,
  tagging: Tagging,
  innerType: Asn1Type
) extends Asn1Type

/** Reference to a named type in the same or another module. Resolved by SchemaRegistry. */
case class Asn1TypeReference(
  moduleName: Option[String],
  typeName: String
) extends Asn1Type

// Supporting types -------------------------------------------------------

case class ComponentType(
  name: String,
  asn1Type: Asn1Type,
  optional: Boolean,
  default: Option[String]
)

case class NamedType(name: String, asn1Type: Asn1Type)

case class NamedBit(name: String, index: Int)

case class EnumerationItem(name: String, value: Option[Long])

sealed trait TagClass
object TagClass {
  case object Universal       extends TagClass
  case object Application     extends TagClass
  case object ContextSpecific extends TagClass
  case object Private         extends TagClass
}

sealed trait Tagging
object Tagging {
  case object Implicit  extends Tagging
  case object Explicit  extends Tagging
  case object Automatic extends Tagging
}
