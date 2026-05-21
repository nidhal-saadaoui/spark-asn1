package io.github.sparkasn1.spark.asn1.codec

import io.github.sparkasn1.spark.asn1.model._
import io.github.sparkasn1.spark.asn1.parser.SchemaRegistry
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.bouncycastle.asn1._

import scala.util.Try

/**
 * Encodes Spark InternalRow values to BER/DER bytes using the provided ASN.1 schema.
 *
 * Type mapping (reverse of BerDerDecoder):
 *   LongType      → ASN1Integer
 *   BooleanType   → ASN1Boolean
 *   StringType    → DERUTF8String  (or ASN1ObjectIdentifier / ASN1RelativeOID for OID fields)
 *   BinaryType    → DEROctetString (unnamed BIT STRING encoded the same way)
 *   NullType      → DERNull
 *   StructType    → DERSequence (SEQUENCE/SET) or CHOICE active alternative
 *   ArrayType     → DERSequence (SEQUENCE OF / SET OF)
 *   ENUMERATED    → ASN1Enumerated (string name resolved back to integer index)
 *
 * OPTIONAL fields that are null are omitted from the output.
 */
class BerDerEncoder(
  registry:       SchemaRegistry,
  moduleName:     String,
  choiceTagField: String = "_tag"
) {

  /** Encode a top-level row against `asn1Schema` and return DER bytes. */
  def encodeRow(row: InternalRow, sparkSchema: StructType, asn1Schema: Asn1Type): Array[Byte] =
    encodeValue(row, sparkSchema, asn1Schema).toASN1Primitive.getEncoded("DER")

  // -------------------------------------------------------------------------
  // Core dispatch
  // -------------------------------------------------------------------------

  def encodeValue(value: Any, sparkType: DataType, asn1Type: Asn1Type): ASN1Encodable = {
    if (value == null) return DERNull.INSTANCE
    val eff = resolveRefs(asn1Type)
    eff match {
      case Asn1Boolean =>
        if (value.asInstanceOf[Boolean]) ASN1Boolean.TRUE else ASN1Boolean.FALSE

      case _: Asn1Integer =>
        new ASN1Integer(value.asInstanceOf[Long])

      case Asn1OctetString =>
        new DEROctetString(value.asInstanceOf[Array[Byte]])

      case Asn1ObjectIdentifier =>
        new ASN1ObjectIdentifier(value.asInstanceOf[UTF8String].toString)

      case Asn1RelativeOid =>
        new ASN1RelativeOID(value.asInstanceOf[UTF8String].toString)

      case Asn1Null =>
        DERNull.INSTANCE

      case _: Asn1StringType =>
        new DERUTF8String(value.asInstanceOf[UTF8String].toString)

      case e: Asn1Enumerated =>
        sparkType match {
          case LongType =>
            new ASN1Enumerated(value.asInstanceOf[Long].toInt)
          case _ =>
            val name   = value.asInstanceOf[UTF8String].toString
            val intVal = e.values.find(_.name == name).flatMap(_.value).getOrElse(0L)
            new ASN1Enumerated(intVal.toInt)
        }

      case b: Asn1BitString =>
        sparkType match {
          case BinaryType =>
            new DERBitString(value.asInstanceOf[Array[Byte]])
          case st: StructType =>
            val row  = value.asInstanceOf[InternalRow]
            val bIdx = Try(st.fieldIndex("_bytes")).getOrElse(0)
            new DERBitString(row.getBinary(bIdx))
          case _ =>
            new DERBitString(Array.empty[Byte])
        }

      case s: Asn1Sequence =>
        encodeSequenceComponents(value.asInstanceOf[InternalRow],
          sparkType.asInstanceOf[StructType], s.components, isSet = false)

      case s: Asn1Set =>
        encodeSequenceComponents(value.asInstanceOf[InternalRow],
          sparkType.asInstanceOf[StructType], s.components, isSet = true)

      case so: Asn1SequenceOf =>
        encodeArrayOf(value.asInstanceOf[ArrayData],
          sparkType.asInstanceOf[ArrayType].elementType, so.elementType, isSet = false)

      case so: Asn1SetOf =>
        encodeArrayOf(value.asInstanceOf[ArrayData],
          sparkType.asInstanceOf[ArrayType].elementType, so.elementType, isSet = true)

      case c: Asn1Choice =>
        encodeChoice(value.asInstanceOf[InternalRow], sparkType.asInstanceOf[StructType], c)

      case _ =>
        new DEROctetString(Array.empty[Byte])
    }
  }

  // -------------------------------------------------------------------------
  // Constructed type encoders
  // -------------------------------------------------------------------------

  private val autoTags: Boolean =
    registry.modules.get(moduleName).exists(_.tagDefault == TagDefault.Automatic)

  private def encodeSequenceComponents(
    row:        InternalRow,
    st:         StructType,
    components: Seq[ComponentType],
    isSet:      Boolean
  ): ASN1Primitive = {
    val vec = new ASN1EncodableVector()
    components.zipWithIndex.foreach { case (comp, compIdx) =>
      val fieldIdx = Try(st.fieldIndex(comp.name)).getOrElse(-1)
      if (fieldIdx >= 0) {
        val field = st(fieldIdx)
        val v     = row.get(fieldIdx, field.dataType)
        if (v != null || !comp.optional) {
          val encoded = encodeValue(v, field.dataType, comp.asn1Type)
          if (autoTags) {
            // CHOICE has no universal tag, so per X.680 it needs EXPLICIT tagging;
            // all other types use IMPLICIT tagging.
            val needsExplicit = resolveRefs(comp.asn1Type).isInstanceOf[Asn1Choice]
            vec.add(new DERTaggedObject(needsExplicit, compIdx, encoded))
          } else
            vec.add(encoded)
        }
        // null + optional → omit (absent OPTIONAL)
      }
    }
    if (isSet) new DERSet(vec) else new DERSequence(vec)
  }

  private def encodeArrayOf(
    arr:             ArrayData,
    elemSparkType:   DataType,
    elemAsn1Type:    Asn1Type,
    isSet:           Boolean
  ): ASN1Primitive = {
    val vec = new ASN1EncodableVector()
    (0 until arr.numElements()).foreach { i =>
      val v = arr.get(i, elemSparkType)
      vec.add(encodeValue(v, elemSparkType, elemAsn1Type))
    }
    if (isSet) new DERSet(vec) else new DERSequence(vec)
  }

  private def encodeChoice(row: InternalRow, st: StructType, choice: Asn1Choice): ASN1Primitive = {
    val tagIdx  = Try(st.fieldIndex(choiceTagField)).getOrElse(0)
    val tagName = row.get(tagIdx, StringType).asInstanceOf[UTF8String].toString
    val altOpt  = choice.alternatives.find(_.name == tagName)
    altOpt match {
      case None => DERNull.INSTANCE
      case Some(alt) =>
        val fieldIdx = Try(st.fieldIndex(alt.name)).getOrElse(-1)
        if (fieldIdx < 0) DERNull.INSTANCE
        else {
          val field = st(fieldIdx)
          val v     = row.get(fieldIdx, field.dataType)
          if (v == null) DERNull.INSTANCE
          else encodeValue(v, field.dataType, alt.asn1Type).toASN1Primitive
        }
    }
  }

  // -------------------------------------------------------------------------
  // Reference resolution (mirrors BerDerDecoder)
  // -------------------------------------------------------------------------

  private def resolveRefs(t: Asn1Type): Asn1Type = t match {
    case ref: Asn1TypeReference =>
      val resolved = registry.resolveRef(ref, moduleName)
      if (resolved == ref) ref else resolveRefs(resolved)
    case _ => t
  }
}
