package io.github.sparkasn1.spark.asn1.codec

import io.github.sparkasn1.spark.asn1.model._
import io.github.sparkasn1.spark.asn1.parser.SchemaRegistry
import io.github.sparkasn1.spark.asn1.util.BerRealUtil
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.bouncycastle.asn1._

import java.io.ByteArrayOutputStream
import scala.util.Try

/**
 * Encodes Spark InternalRow values to DER bytes using the provided ASN.1 schema.
 *
 * Returns raw byte arrays rather than BouncyCastle ASN1Encodable objects so that
 * REAL (tag 9) can be handled without BouncyCastle support for that type.
 */
class BerDerEncoder(
  registry:       SchemaRegistry,
  moduleName:     String,
  choiceTagField: String = "_tag"
) {

  /** Encode a top-level row against `asn1Schema` and return DER bytes. */
  def encodeRow(row: InternalRow, sparkSchema: StructType, asn1Schema: Asn1Type): Array[Byte] =
    encodeDer(row, sparkSchema, asn1Schema)

  // -------------------------------------------------------------------------
  // Core dispatch — returns raw DER TLV bytes
  // -------------------------------------------------------------------------

  def encodeDer(value: Any, sparkType: DataType, asn1Type: Asn1Type): Array[Byte] = {
    if (value == null) return DERNull.INSTANCE.getEncoded("DER")
    val eff = resolveRefs(asn1Type)
    eff match {
      case Asn1Boolean =>
        (if (value.asInstanceOf[Boolean]) ASN1Boolean.TRUE else ASN1Boolean.FALSE).getEncoded("DER")

      case _: Asn1Integer =>
        new ASN1Integer(value.asInstanceOf[Long]).getEncoded("DER")

      case Asn1OctetString =>
        new DEROctetString(value.asInstanceOf[Array[Byte]]).getEncoded("DER")

      case Asn1ObjectIdentifier =>
        new ASN1ObjectIdentifier(value.asInstanceOf[UTF8String].toString).getEncoded("DER")

      case Asn1RelativeOid =>
        new ASN1RelativeOID(value.asInstanceOf[UTF8String].toString).getEncoded("DER")

      case Asn1Null =>
        DERNull.INSTANCE.getEncoded("DER")

      case _: Asn1StringType =>
        new DERUTF8String(value.asInstanceOf[UTF8String].toString).getEncoded("DER")

      case e: Asn1Enumerated =>
        val intVal: Int = sparkType match {
          case LongType => value.asInstanceOf[Long].toInt
          case _ =>
            val name = value.asInstanceOf[UTF8String].toString
            e.values.find(_.name == name).flatMap(_.value).getOrElse(0L).toInt
        }
        new ASN1Enumerated(intVal).getEncoded("DER")

      case b: Asn1BitString =>
        val bytes: Array[Byte] = sparkType match {
          case BinaryType => value.asInstanceOf[Array[Byte]]
          case st: StructType =>
            val row  = value.asInstanceOf[InternalRow]
            val bIdx = Try(st.fieldIndex("_bytes")).getOrElse(0)
            row.getBinary(bIdx)
          case _ => Array.empty[Byte]
        }
        new DERBitString(bytes).getEncoded("DER")

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

      case Asn1Real =>
        BerRealUtil.buildTlv(BerRealUtil.encodeContent(value.asInstanceOf[Double]))

      case Asn1Any =>
        new DEROctetString(value.asInstanceOf[Array[Byte]]).getEncoded("DER")

      case _ =>
        new DEROctetString(Array.empty[Byte]).getEncoded("DER")
    }
  }

  // Keep the old name as a bridge for any internal call-sites that still use it.
  def encodeValue(value: Any, sparkType: DataType, asn1Type: Asn1Type): Array[Byte] =
    encodeDer(value, sparkType, asn1Type)

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
  ): Array[Byte] = {
    val content = new ByteArrayOutputStream()
    components.zipWithIndex.foreach { case (comp, compIdx) =>
      val fieldIdx = Try(st.fieldIndex(comp.name)).getOrElse(-1)
      if (fieldIdx >= 0) {
        val field = st(fieldIdx)
        val v     = row.get(fieldIdx, field.dataType)
        if (v != null || !comp.optional) {
          val innerDer = encodeDer(v, field.dataType, comp.asn1Type)
          val finalDer =
            if (autoTags) {
              val needsExplicit = resolveRefs(comp.asn1Type).isInstanceOf[Asn1Choice]
              if (needsExplicit) applyExplicitTag(compIdx, innerDer)
              else               applyImplicitTag(compIdx, innerDer)
            } else innerDer
          content.write(finalDer)
        }
        // null + optional → omit (absent OPTIONAL)
      }
    }
    val outerTag = if (isSet) 0x31 else 0x30
    derTlv(outerTag, content.toByteArray)
  }

  private def encodeArrayOf(
    arr:           ArrayData,
    elemSparkType: DataType,
    elemAsn1Type:  Asn1Type,
    isSet:         Boolean
  ): Array[Byte] = {
    val content = new ByteArrayOutputStream()
    (0 until arr.numElements()).foreach { i =>
      content.write(encodeDer(arr.get(i, elemSparkType), elemSparkType, elemAsn1Type))
    }
    val outerTag = if (isSet) 0x31 else 0x30
    derTlv(outerTag, content.toByteArray)
  }

  private def encodeChoice(row: InternalRow, st: StructType, choice: Asn1Choice): Array[Byte] = {
    val tagIdx  = Try(st.fieldIndex(choiceTagField)).getOrElse(0)
    val tagName = row.get(tagIdx, StringType).asInstanceOf[UTF8String].toString
    choice.alternatives.find(_.name == tagName) match {
      case None => DERNull.INSTANCE.getEncoded("DER")
      case Some(alt) =>
        val fieldIdx = Try(st.fieldIndex(alt.name)).getOrElse(-1)
        if (fieldIdx < 0) DERNull.INSTANCE.getEncoded("DER")
        else {
          val field = st(fieldIdx)
          val v     = row.get(fieldIdx, field.dataType)
          if (v == null) DERNull.INSTANCE.getEncoded("DER")
          else encodeDer(v, field.dataType, alt.asn1Type)
        }
    }
  }

  // -------------------------------------------------------------------------
  // Tag manipulation helpers
  // -------------------------------------------------------------------------

  /** Build a BER/DER TLV: tag byte + BER length + content. */
  private def derTlv(tag: Int, content: Array[Byte]): Array[Byte] = {
    val len = content.length
    val lenBytes: Array[Byte] =
      if (len <= 127)   Array(len.toByte)
      else if (len <= 255) Array(0x81.toByte, len.toByte)
      else              Array(0x82.toByte, (len >> 8).toByte, (len & 0xff).toByte)
    val result = new Array[Byte](1 + lenBytes.length + len)
    result(0) = tag.toByte
    System.arraycopy(lenBytes, 0, result, 1, lenBytes.length)
    System.arraycopy(content, 0, result, 1 + lenBytes.length, len)
    result
  }

  /**
   * Apply IMPLICIT context-specific tag `n` to `innerDer`.
   * Replaces the outer tag byte; the length and content bytes stay the same.
   */
  private def applyImplicitTag(n: Int, innerDer: Array[Byte]): Array[Byte] = {
    if (innerDer.isEmpty) return innerDer
    val isConstructed = (innerDer(0) & 0x20) != 0
    val newTag = 0x80 | (if (isConstructed) 0x20 else 0) | n
    val result = innerDer.clone()
    result(0) = newTag.toByte
    result
  }

  /**
   * Apply EXPLICIT context-specific CONSTRUCTED tag `n` to `innerDer`.
   * Wraps the full inner TLV in a new TLV.
   */
  private def applyExplicitTag(n: Int, innerDer: Array[Byte]): Array[Byte] =
    derTlv(0x80 | 0x20 | n, innerDer)

  // -------------------------------------------------------------------------
  // Reference resolution
  // -------------------------------------------------------------------------

  private def resolveRefs(t: Asn1Type): Asn1Type = t match {
    case ref: Asn1TypeReference =>
      val resolved = registry.resolveRef(ref, moduleName)
      if (resolved == ref) ref else resolveRefs(resolved)
    case _ => t
  }
}
