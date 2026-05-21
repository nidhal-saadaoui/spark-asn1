package io.github.sparkasn1.spark.asn1.codec.xer

import io.github.sparkasn1.spark.asn1.model._
import io.github.sparkasn1.spark.asn1.parser.SchemaRegistry
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import scala.util.Try

/**
 * Schema-driven XER encoder. Produces Basic XER (BXER) output.
 *
 * Each record is wrapped in `<rootTag>…</rootTag>` followed by a newline,
 * so concatenated records form a valid XerRecordIterator input stream.
 */
class XerEncoder(
  registry:       SchemaRegistry,
  moduleName:     String,
  choiceTagField: String = "_tag"
) {

  def encodeRow(
    row:        InternalRow,
    sparkSchema: StructType,
    asn1Schema: Asn1Type,
    rootTag:    String
  ): Array[Byte] = {
    val sb = new StringBuilder
    sb.append('<').append(rootTag).append('>')
    encodeValue(sb, row, sparkSchema, asn1Schema)
    sb.append("</").append(rootTag).append(">\n")
    sb.toString().getBytes("UTF-8")
  }

  // -------------------------------------------------------------------------
  // Core dispatch
  // -------------------------------------------------------------------------

  private def encodeValue(sb: StringBuilder, value: Any, sparkType: DataType, rawAsn1Type: Asn1Type): Unit = {
    if (value == null) return
    val t = resolveRefs(rawAsn1Type)
    t match {
      case Asn1Null => ()

      case Asn1Boolean =>
        if (value.asInstanceOf[Boolean]) sb.append("<true/>") else sb.append("<false/>")

      case _: Asn1Integer =>
        sb.append(value.asInstanceOf[Long])

      case Asn1Real =>
        val d = value.asInstanceOf[Double]
        if (d.isInfinite && d > 0) sb.append("<PLUS-INFINITY/>")
        else if (d.isInfinite)     sb.append("<MINUS-INFINITY/>")
        else if (d.isNaN)          sb.append("<NOT-A-NUMBER/>")
        else                       sb.append(d)

      case Asn1OctetString =>
        sb.append(bytesToHex(value.asInstanceOf[Array[Byte]]))

      case Asn1ObjectIdentifier | Asn1RelativeOid =>
        sb.append(escapeXml(value.asInstanceOf[UTF8String].toString))

      case _: Asn1StringType =>
        sb.append(escapeXml(value.asInstanceOf[UTF8String].toString))

      case e: Asn1Enumerated =>
        val name = sparkType match {
          case LongType =>
            val idx = value.asInstanceOf[Long].toInt
            if (idx < e.values.size) e.values(idx).name else idx.toString
          case _ =>
            value.asInstanceOf[UTF8String].toString
        }
        sb.append('<').append(name).append("/>")

      case b: Asn1BitString =>
        val bytes: Array[Byte] = sparkType match {
          case st: StructType =>
            val row  = value.asInstanceOf[InternalRow]
            val bIdx = Try(st.fieldIndex("_bytes")).getOrElse(0)
            row.getBinary(bIdx)
          case _ =>
            value.asInstanceOf[Array[Byte]]
        }
        sb.append(bytesToHex(bytes))

      case s: Asn1Sequence =>
        encodeSequenceComps(sb, value.asInstanceOf[InternalRow], sparkType.asInstanceOf[StructType], s.components)

      case s: Asn1Set =>
        encodeSequenceComps(sb, value.asInstanceOf[InternalRow], sparkType.asInstanceOf[StructType], s.components)

      case so: Asn1SequenceOf =>
        encodeArrayOf(sb, value.asInstanceOf[ArrayData], sparkType.asInstanceOf[ArrayType], so.elementType)

      case so: Asn1SetOf =>
        encodeArrayOf(sb, value.asInstanceOf[ArrayData], sparkType.asInstanceOf[ArrayType], so.elementType)

      case c: Asn1Choice =>
        encodeChoice(sb, value.asInstanceOf[InternalRow], sparkType.asInstanceOf[StructType], c)

      case tt: Asn1TaggedType =>
        encodeValue(sb, value, sparkType, tt.innerType)

      case _ => ()
    }
  }

  // -------------------------------------------------------------------------
  // Constructed type encoders
  // -------------------------------------------------------------------------

  private def encodeSequenceComps(
    sb:    StringBuilder,
    row:   InternalRow,
    st:    StructType,
    comps: Seq[ComponentType]
  ): Unit =
    comps.foreach { comp =>
      val idx = Try(st.fieldIndex(comp.name)).getOrElse(-1)
      if (idx >= 0) {
        val v = row.get(idx, st(idx).dataType)
        if (v != null) {
          sb.append('<').append(comp.name).append('>')
          encodeValue(sb, v, st(idx).dataType, comp.asn1Type)
          sb.append("</").append(comp.name).append('>')
        }
      }
    }

  private def encodeArrayOf(
    sb:            StringBuilder,
    arr:           ArrayData,
    at:            ArrayType,
    elemAsn1Type:  Asn1Type
  ): Unit =
    (0 until arr.numElements()).foreach { i =>
      val v = arr.get(i, at.elementType)
      sb.append("<item>")
      encodeValue(sb, v, at.elementType, elemAsn1Type)
      sb.append("</item>")
    }

  private def encodeChoice(
    sb:      StringBuilder,
    row:     InternalRow,
    st:      StructType,
    choice:  Asn1Choice
  ): Unit = {
    val tagIdx  = Try(st.fieldIndex(choiceTagField)).getOrElse(0)
    val tagName = row.get(tagIdx, StringType).asInstanceOf[UTF8String].toString
    choice.alternatives.find(_.name == tagName).foreach { alt =>
      val idx = Try(st.fieldIndex(alt.name)).getOrElse(-1)
      if (idx >= 0) {
        val v = row.get(idx, st(idx).dataType)
        if (v != null) {
          sb.append('<').append(alt.name).append('>')
          encodeValue(sb, v, st(idx).dataType, alt.asn1Type)
          sb.append("</").append(alt.name).append('>')
        }
      }
    }
  }

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  private def resolveRefs(t: Asn1Type): Asn1Type = t match {
    case ref: Asn1TypeReference =>
      val resolved = registry.resolveRef(ref, moduleName)
      if (resolved eq ref) ref else resolveRefs(resolved)
    case _ => t
  }

  private def bytesToHex(bytes: Array[Byte]): String = {
    val sb = new StringBuilder(bytes.length * 2)
    bytes.foreach(b => sb.append(f"${b & 0xff}%02X"))
    sb.toString()
  }

  private def escapeXml(s: String): String =
    s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
}
