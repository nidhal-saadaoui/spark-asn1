package io.github.sparkasn1.spark.asn1.model

case class TypeAssignment(name: String, asn1Type: Asn1Type)

case class ValueAssignment(name: String, asn1Type: Asn1Type, rawValue: String)

case class SymbolsFromModule(symbols: Seq[String], fromModule: String)

sealed trait TagDefault
object TagDefault {
  case object Explicit  extends TagDefault
  case object Implicit  extends TagDefault
  case object Automatic extends TagDefault
  case object None      extends TagDefault
}

case class Asn1Module(
  name: String,
  tagDefault: TagDefault,
  extensibilityImplied: Boolean,
  imports: Seq[SymbolsFromModule],
  typeAssignments: Map[String, TypeAssignment],
  valueAssignments: Map[String, ValueAssignment]
)
