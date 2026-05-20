package io.github.sparkasn1.spark.asn1.codec.per

import io.github.sparkasn1.spark.asn1.model._
import io.github.sparkasn1.spark.asn1.parser.SchemaRegistry
import io.github.sparkasn1.spark.asn1.util.BitUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import java.io.ByteArrayOutputStream
import scala.util.Try

/**
 * Schema-driven PER encoder (reverse of PerDecoder).
 *
 * @param aligned true for Aligned PER (APER), false for Unaligned PER (UPER)
 */
class PerEncoder(
  registry:       SchemaRegistry,
  moduleName:     String,
  val aligned:    Boolean,
  choiceTagField: String = "_tag"
) {

  /** Encode a top-level row and return the raw PER bytes (no framing). */
  def encodeRow(row: InternalRow, sparkSchema: StructType, asn1Schema: Asn1Type): Array[Byte] = {
    val buf = new PerBitOutput(aligned)
    encodeValue(buf, row, sparkSchema, asn1Schema)
    buf.toByteArray
  }

  // -------------------------------------------------------------------------
  // Core dispatch
  // -------------------------------------------------------------------------

  private def encodeValue(buf: PerBitOutput, value: Any, sparkType: DataType, rawAsn1Type: Asn1Type): Unit = {
    val t = resolveRefs(rawAsn1Type)
    if (value == null) return
    t match {
      case Asn1Null => ()

      case Asn1Boolean =>
        if (aligned) {
          buf.align()
          buf.writeBits(if (value.asInstanceOf[Boolean]) 0xffL else 0x00L, 8)
        } else {
          buf.writeBit(if (value.asInstanceOf[Boolean]) 1 else 0)
        }

      case i: Asn1Integer =>
        encodeInteger(buf, value.asInstanceOf[Long], i.constraint)

      case Asn1OctetString =>
        val bytes = value.asInstanceOf[Array[Byte]]
        writeLengthDet(buf, bytes.length)
        buf.writeBytes(bytes)

      case b: Asn1BitString =>
        encodeBitString(buf, value, sparkType, b)

      case Asn1ObjectIdentifier | Asn1RelativeOid =>
        val oidBytes = oidStringToBerBytes(value.asInstanceOf[UTF8String].toString)
        writeLengthDet(buf, oidBytes.length)
        buf.writeBytes(oidBytes)

      case _: Asn1StringType =>
        val utf8 = value.asInstanceOf[UTF8String].toString.getBytes("UTF-8")
        writeLengthDet(buf, utf8.length)
        buf.writeBytes(utf8)

      case e: Asn1Enumerated =>
        encodeEnumerated(buf, value, sparkType, e)

      case s: Asn1Sequence =>
        encodeSequence(buf, value.asInstanceOf[InternalRow], sparkType.asInstanceOf[StructType], s)

      case s: Asn1Set =>
        encodeSequence(buf, value.asInstanceOf[InternalRow], sparkType.asInstanceOf[StructType],
          Asn1Sequence(s.components, s.extensible))

      case so: Asn1SequenceOf =>
        encodeSequenceOf(buf, value.asInstanceOf[ArrayData], sparkType.asInstanceOf[ArrayType], so)

      case so: Asn1SetOf =>
        encodeSequenceOf(buf, value.asInstanceOf[ArrayData], sparkType.asInstanceOf[ArrayType],
          Asn1SequenceOf(so.elementType, so.sizeConstraint))

      case c: Asn1Choice =>
        encodeChoice(buf, value.asInstanceOf[InternalRow], sparkType.asInstanceOf[StructType], c)

      case tt: Asn1TaggedType =>
        encodeValue(buf, value, sparkType, tt.innerType)

      case _ => ()
    }
  }

  // -------------------------------------------------------------------------
  // Primitives
  // -------------------------------------------------------------------------

  private def encodeInteger(buf: PerBitOutput, value: Long, constraint: Option[ValueRangeConstraint]): Unit =
    constraint match {
      case Some(ValueRangeConstraint(min, max)) if max != Long.MaxValue =>
        val range = max - min + 1
        if (range > 1L) {
          val bits = BitUtils.bitsForRange(min, max)
          if (aligned && bits > 8) buf.align()
          buf.writeBits(value - min, bits)
        }
      case Some(ValueRangeConstraint(min, _)) =>
        encodeUnconstrained(buf, value - min)
      case None =>
        encodeUnconstrained(buf, value)
    }

  private def encodeUnconstrained(buf: PerBitOutput, value: Long): Unit = {
    buf.align()
    val bytes = longToMinimalBytes(value)
    writeLengthDet(buf, bytes.length)
    buf.writeBytes(bytes)
  }

  private def encodeEnumerated(buf: PerBitOutput, value: Any, sparkType: DataType, schema: Asn1Enumerated): Unit = {
    val count = schema.values.size
    val idx: Int = sparkType match {
      case LongType => value.asInstanceOf[Long].toInt
      case _ =>
        val name = value.asInstanceOf[UTF8String].toString
        val i    = schema.values.indexWhere(_.name == name)
        if (i >= 0) i else 0
    }
    if (aligned) buf.align()
    buf.writeBits(idx.toLong, BitUtils.ceilLog2(count))
  }

  private def encodeBitString(buf: PerBitOutput, value: Any, sparkType: DataType, schema: Asn1BitString): Unit = {
    val bytes: Array[Byte] = sparkType match {
      case st: StructType =>
        val row  = value.asInstanceOf[InternalRow]
        val bIdx = Try(st.fieldIndex("_bytes")).getOrElse(0)
        row.getBinary(bIdx)
      case _ => value.asInstanceOf[Array[Byte]]
    }
    schema.sizeConstraint match {
      case Some(SizeConstraint(min, max)) if min == max => ()  // fixed size: no length field
      case c => writeConstrainedLen(buf, bytes.length.toLong * 8, c)
    }
    buf.align()
    bytes.foreach(b => buf.writeBits(b & 0xff, 8))
  }

  // -------------------------------------------------------------------------
  // Constructed types
  // -------------------------------------------------------------------------

  private def encodeSequence(buf: PerBitOutput, row: InternalRow, st: StructType, schema: Asn1Sequence): Unit = {
    if (schema.extensible) buf.writeBit(0)  // extension bit: no extensions

    val optionals = schema.components.filter(c => c.optional || c.default.isDefined)
    optionals.foreach { comp =>
      val idx     = Try(st.fieldIndex(comp.name)).getOrElse(-1)
      val present = idx >= 0 && row.get(idx, st(idx).dataType) != null
      buf.writeBit(if (present) 1 else 0)
    }
    if (aligned) buf.align()

    schema.components.foreach { comp =>
      val isOpt = comp.optional || comp.default.isDefined
      val idx   = Try(st.fieldIndex(comp.name)).getOrElse(-1)
      if (idx >= 0) {
        val v = row.get(idx, st(idx).dataType)
        if (!isOpt || v != null)
          encodeValue(buf, v, st(idx).dataType, comp.asn1Type)
      }
    }
  }

  private def encodeSequenceOf(buf: PerBitOutput, arr: ArrayData, at: ArrayType, schema: Asn1SequenceOf): Unit = {
    writeConstrainedLen(buf, arr.numElements().toLong, schema.sizeConstraint)
    if (aligned) buf.align()
    (0 until arr.numElements()).foreach { i =>
      encodeValue(buf, arr.get(i, at.elementType), at.elementType, schema.elementType)
    }
  }

  private def encodeChoice(buf: PerBitOutput, row: InternalRow, st: StructType, choice: Asn1Choice): Unit = {
    if (choice.extensible) buf.writeBit(0)  // no extension
    val n    = choice.alternatives.size
    val bits = BitUtils.ceilLog2(n)
    val tagIdx  = Try(st.fieldIndex(choiceTagField)).getOrElse(0)
    val tagName = row.get(tagIdx, StringType).asInstanceOf[UTF8String].toString
    val altIdx  = choice.alternatives.indexWhere(_.name == tagName)
    if (aligned && bits > 0) buf.align()
    buf.writeBits(if (altIdx >= 0) altIdx.toLong else 0L, bits)
    if (altIdx >= 0) {
      val alt  = choice.alternatives(altIdx)
      val fIdx = Try(st.fieldIndex(alt.name)).getOrElse(-1)
      if (fIdx >= 0) encodeValue(buf, row.get(fIdx, st(fIdx).dataType), st(fIdx).dataType, alt.asn1Type)
    }
  }

  // -------------------------------------------------------------------------
  // Length determinants
  // -------------------------------------------------------------------------

  private def writeLengthDet(buf: PerBitOutput, len: Int): Unit = {
    buf.align()
    if (len <= 127) {
      buf.writeBits(len.toLong, 8)
    } else if (len <= 16383) {
      buf.writeBits(0x8000L | len.toLong, 16)
    } else {
      throw new UnsupportedOperationException(s"PER fragmented length ($len) not supported")
    }
  }

  private def writeConstrainedLen(buf: PerBitOutput, count: Long, constraint: Option[SizeConstraint]): Unit =
    constraint match {
      case Some(SizeConstraint(min, max)) if min == max => ()
      case Some(SizeConstraint(min, max)) if max != Long.MaxValue =>
        val bits = BitUtils.bitsForRange(min, max)
        if (aligned && bits > 8) buf.align()
        buf.writeBits(count - min, bits)
      case _ =>
        writeLengthDet(buf, count.toInt)
    }

  // -------------------------------------------------------------------------
  // Byte / OID helpers
  // -------------------------------------------------------------------------

  private def longToMinimalBytes(value: Long): Array[Byte] = {
    if (value == 0L) return Array[Byte](0)
    val buf = new Array[Byte](8)
    var v   = value
    var i   = 7
    while (i >= 0) { buf(i) = (v & 0xff).toByte; v >>= 8; i -= 1 }
    // Strip sign-extension prefix bytes (but keep at least 1 byte)
    var start = 0
    while (start < 7 &&
      ((buf(start) == 0x00.toByte && (buf(start + 1) & 0x80) == 0) ||
       (buf(start) == 0xff.toByte && (buf(start + 1) & 0x80) != 0))) {
      start += 1
    }
    buf.slice(start, 8)
  }

  private def oidStringToBerBytes(oid: String): Array[Byte] = {
    val parts = oid.split("\\.").map(s => java.lang.Long.parseLong(s))
    if (parts.length < 2) return Array.empty
    val out = new ByteArrayOutputStream()
    out.write(encodeOidComponent(parts(0) * 40 + parts(1)))
    (2 until parts.length).foreach(i => out.write(encodeOidComponent(parts(i))))
    out.toByteArray
  }

  private def encodeOidComponent(value: Long): Array[Byte] = {
    if (value == 0L) return Array[Byte](0)
    val stack = scala.collection.mutable.ArrayBuffer[Byte]()
    var v     = value
    stack += (v & 0x7f).toByte
    v >>= 7
    while (v > 0) { stack += ((v & 0x7f) | 0x80).toByte; v >>= 7 }
    stack.reverse.toArray
  }

  private def resolveRefs(t: Asn1Type): Asn1Type = t match {
    case ref: Asn1TypeReference =>
      val resolved = registry.resolveRef(ref, moduleName)
      if (resolved eq ref) ref else resolveRefs(resolved)
    case _ => t
  }
}
