package io.github.sparkasn1.spark.asn1.codec.per

import io.github.sparkasn1.spark.asn1.model._
import io.github.sparkasn1.spark.asn1.parser.SchemaRegistry
import io.github.sparkasn1.spark.asn1.util.BitUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.unsafe.types.UTF8String

/**
 * Schema-driven PER decoder.
 *
 * @param registry resolved schema registry for cross-module type references
 * @param moduleName the module containing the root type being decoded
 * @param aligned true for Aligned PER (APER), false for Unaligned PER (UPER)
 *
 * Encoding rules implemented (X.691 subset):
 *  - NULL: 0 bits
 *  - BOOLEAN: 1 bit (UPER) / 1 byte (APER)
 *  - INTEGER constrained: bitsForRange bits as offset from lower bound
 *  - INTEGER unconstrained: length-determinant + big-endian two's-complement bytes
 *  - ENUMERATED: index in ceilLog2(count) bits; APER aligns before read
 *  - OCTET STRING fixed-size: raw bytes; variable: length-det + bytes
 *  - BIT STRING fixed-size: raw bits (APER byte-aligned); variable: length-det + bits
 *  - String types: length-det (octets) + UTF-8 bytes; APER aligns before each
 *  - SEQUENCE: extension bit (if extensible) + presence bitmap + fields
 *  - SEQUENCE OF / SET OF: constrained count or length-det, then elements
 *  - CHOICE: extension bit (if extensible) + index + value
 *  - OBJECT IDENTIFIER: length-det + BER-encoded OID content bytes (dot-notation output)
 *  - Tagged types: inner type is decoded directly (tags carry no bits in PER)
 */
class PerDecoder(
  registry:        SchemaRegistry,
  moduleName:      String,
  val aligned:     Boolean,
  enumeratedAsInt: Boolean = false
) {

  /** Decode a complete record from a byte array. */
  def decodeBytes(bytes: Array[Byte], schema: Asn1Type): InternalRow = {
    val buf = PerBitBuffer(bytes)
    decodeToRow(buf, resolveRefs(schema))
  }

  // -------------------------------------------------------------------------
  // Top-level dispatch
  // -------------------------------------------------------------------------

  private def decodeToRow(buf: PerBitBuffer, schema: Asn1Type): InternalRow =
    schema match {
      case s: Asn1Sequence  => decodeSequence(buf, s)
      case s: Asn1Set       => decodeSequence(buf, Asn1Sequence(s.components, s.extensible))
      case c: Asn1Choice    => decodeChoice(buf, c)
      case _                => new GenericInternalRow(Array[Any](decodeValue(buf, schema)))
    }

  /** Decode one field value; returns Any compatible with Spark InternalRow. */
  private def decodeValue(buf: PerBitBuffer, rawSchema: Asn1Type): Any = {
    val schema = resolveRefs(rawSchema)
    schema match {
      case Asn1Null              => null
      case Asn1Boolean           => decodeBoolean(buf)
      case i: Asn1Integer        => decodeInteger(buf, i.constraint)
      case Asn1OctetString       => decodeOctetString(buf, None)
      case b: Asn1BitString      => decodeBitString(buf, b)
      case Asn1ObjectIdentifier  => decodeOid(buf)
      case Asn1RelativeOid       => decodeOid(buf)
      case _: Asn1StringType     => decodeString(buf)
      case e: Asn1Enumerated     => decodeEnumerated(buf, e)
      case s: Asn1Sequence       => decodeSequence(buf, s)
      case s: Asn1Set            => decodeSequence(buf, Asn1Sequence(s.components, s.extensible))
      case so: Asn1SequenceOf    => decodeSequenceOf(buf, so)
      case so: Asn1SetOf         => decodeSequenceOf(buf, Asn1SequenceOf(so.elementType, so.sizeConstraint))
      case c: Asn1Choice         => decodeChoice(buf, c)
      case tt: Asn1TaggedType    => decodeValue(buf, tt.innerType)
      case Asn1Any               => decodeOctetString(buf, None)
      case _: Asn1TypeReference  => null // unresolved ref; schema registry should have resolved this
    }
  }

  // -------------------------------------------------------------------------
  // Primitives
  // -------------------------------------------------------------------------

  private def decodeBoolean(buf: PerBitBuffer): Boolean = {
    if (aligned) { maybeAlign(buf); buf.readBits(8) != 0L }
    else buf.readBit() != 0
  }

  private def decodeInteger(buf: PerBitBuffer, constraint: Option[ValueRangeConstraint]): Long =
    constraint match {
      case Some(ValueRangeConstraint(min, max)) if max != Long.MaxValue =>
        val range = max - min + 1
        if (range == 1L) {
          min
        } else {
          val bits = BitUtils.bitsForRange(min, max)
          if (aligned && bits > 8) maybeAlign(buf)
          buf.readBits(bits) + min
        }
      case Some(ValueRangeConstraint(min, _)) =>
        // Semi-constrained (lower bound only): length-det + offset
        val offset = decodeUnconstrained(buf)
        min + offset
      case None =>
        decodeUnconstrained(buf)
    }

  private def decodeUnconstrained(buf: PerBitBuffer): Long = {
    maybeAlign(buf)
    val len = readLengthDeterminant(buf)
    if (len == 0) return 0L
    maybeAlign(buf)
    // Read len bytes as big-endian two's-complement
    val bytes = buf.readByteArray(len.toInt)
    var result = if ((bytes(0) & 0x80) != 0) -1L else 0L
    bytes.foreach(b => result = (result << 8) | (b & 0xff))
    result
  }

  private def decodeEnumerated(buf: PerBitBuffer, schema: Asn1Enumerated): Any = {
    val count = schema.values.size
    if (count == 0) return if (enumeratedAsInt) 0L else UTF8String.fromString("")
    if (aligned) maybeAlign(buf)
    val idx = buf.readBits(BitUtils.ceilLog2(count)).toInt
    if (enumeratedAsInt) idx.toLong
    else {
      val name = if (idx < schema.values.size) schema.values(idx).name else idx.toString
      UTF8String.fromString(name)
    }
  }

  private def decodeOctetString(buf: PerBitBuffer, sizeConstraint: Option[SizeConstraint]): Array[Byte] = {
    val len = readConstrainedLength(buf, sizeConstraint)
    maybeAlign(buf)
    buf.readByteArray(len.toInt)
  }

  private def decodeBitString(buf: PerBitBuffer, schema: Asn1BitString): Any = {
    val len: Long = schema.sizeConstraint match {
      case Some(SizeConstraint(min, max)) if min == max => min
      case Some(c) => readConstrainedLength(buf, Some(c))
      case None    => readLengthDeterminant(buf)
    }
    // Aligned PER: bit strings are byte-aligned and zero-padded to whole bytes
    val byteLen = ((len + 7) / 8).toInt
    maybeAlign(buf)
    val bytes = buf.readByteArray(byteLen)
    if (schema.namedBits.isEmpty) {
      bytes
    } else {
      val setBitNames: Array[Any] = schema.namedBits.filter { nb =>
        val byteIdx = nb.index / 8
        val bitIdx  = 7 - (nb.index % 8)
        byteIdx < bytes.length && ((bytes(byteIdx) >> bitIdx) & 1) == 1
      }.map(nb => UTF8String.fromString(nb.name): Any).toArray
      new GenericInternalRow(Array[Any](bytes, new GenericArrayData(setBitNames)))
    }
  }

  private def decodeString(buf: PerBitBuffer): UTF8String = {
    val len = readLengthDeterminant(buf)
    maybeAlign(buf)
    val bytes = buf.readByteArray(len.toInt)
    UTF8String.fromBytes(bytes)
  }

  private def decodeOid(buf: PerBitBuffer): UTF8String = {
    // OID in PER: length-det + BER-encoded OID content bytes
    val len   = readLengthDeterminant(buf)
    maybeAlign(buf)
    val bytes = buf.readByteArray(len.toInt)
    UTF8String.fromString(berOidBytesToString(bytes))
  }

  // -------------------------------------------------------------------------
  // Constructed types
  // -------------------------------------------------------------------------

  private def decodeSequence(buf: PerBitBuffer, schema: Asn1Sequence): InternalRow = {
    // Extension bit (if extensible): we only handle the root (non-extended) case
    val hasExtension = if (schema.extensible) buf.readBit() != 0 else false

    val optionals = schema.components.filter(c => c.optional || c.default.isDefined)
    val bitmap: Array[Boolean] =
      if (optionals.isEmpty) Array.empty
      else optionals.indices.map(_ => buf.readBit() != 0).toArray

    if (aligned) maybeAlign(buf) // align after bitmap, before first field

    val result = new Array[Any](schema.components.size)
    var optIdx = 0
    schema.components.zipWithIndex.foreach { case (comp, i) =>
      if (comp.optional || comp.default.isDefined) {
        if (bitmap.lift(optIdx).getOrElse(false))
          result(i) = decodeValue(buf, comp.asn1Type)
        else
          result(i) = null
        optIdx += 1
      } else {
        result(i) = decodeValue(buf, comp.asn1Type)
      }
    }

    // Extension additions are present but we skip them (consume length-det + bytes)
    if (hasExtension) skipExtensionAdditions(buf)

    new GenericInternalRow(result)
  }

  private def decodeSequenceOf(buf: PerBitBuffer, schema: Asn1SequenceOf): GenericArrayData = {
    val count = readConstrainedLength(buf, schema.sizeConstraint)
    if (aligned) maybeAlign(buf)
    val elems = (0L until count).map(_ => decodeValue(buf, schema.elementType)).toArray[Any]
    new GenericArrayData(elems)
  }

  private def decodeChoice(buf: PerBitBuffer, schema: Asn1Choice): InternalRow = {
    val extensionPresent = if (schema.extensible) buf.readBit() != 0 else false
    if (extensionPresent)
      throw new UnsupportedOperationException("PER CHOICE extension additions not yet supported")

    val n     = schema.alternatives.size
    val bits  = BitUtils.ceilLog2(n)
    if (aligned && bits > 0) maybeAlign(buf)
    val idx   = buf.readBits(bits).toInt

    val fields = new Array[Any](1 + n)
    if (idx < n) {
      val alt    = schema.alternatives(idx)
      fields(0)  = UTF8String.fromString(alt.name)
      fields(idx + 1) = decodeValue(buf, alt.asn1Type)
    } else {
      fields(0) = UTF8String.fromString("unknown")
    }
    new GenericInternalRow(fields)
  }

  // -------------------------------------------------------------------------
  // Length determinant (X.691 §11)
  // -------------------------------------------------------------------------

  /**
   * Read a constrained length. If the constraint fixes a single value (min==max),
   * no bits are consumed. For variable-size, reads bitsForRange(min, max) bits.
   * Falls back to an unconstrained length determinant when constraint is absent.
   */
  private def readConstrainedLength(buf: PerBitBuffer, constraint: Option[SizeConstraint]): Long =
    constraint match {
      case Some(SizeConstraint(min, max)) if min == max => min
      case Some(SizeConstraint(min, max)) if max != Long.MaxValue =>
        val bits = BitUtils.bitsForRange(min, max)
        if (aligned && bits > 8) maybeAlign(buf)
        buf.readBits(bits) + min
      case _ =>
        maybeAlign(buf)
        readLengthDeterminant(buf)
    }

  /**
   * Read an unconstrained (or semi-constrained) PER length determinant.
   * 0xxxxxxx (1 byte)  → value 0..127
   * 10xxxxxx xxxxxxxx  → value 0..16383
   * 11xxxxxx …         → fragmentation (not supported)
   */
  private def readLengthDeterminant(buf: PerBitBuffer): Long = {
    maybeAlign(buf)
    val first = buf.readByte()
    if ((first & 0x80) == 0) {
      first.toLong
    } else if ((first & 0xC0) == 0x80) {
      val second = buf.readByte()
      (((first & 0x3f).toLong) << 8) | second.toLong
    } else {
      throw new UnsupportedOperationException(
        "PER fragmented length determinant (≥16384 octets) is not yet supported")
    }
  }

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  private def maybeAlign(buf: PerBitBuffer): Unit = if (aligned) buf.align()

  private def skipExtensionAdditions(buf: PerBitBuffer): Unit = {
    // Read the count of extension additions (normally-small length, §11.7), then skip each
    // as an open type (length-det + bytes). Simplified: read until something makes sense.
    // For Phase 2 we just consume the "normally small" bitmap and open-type wrappers.
    val count = readNormallySmallLength(buf)
    // presence bitmap for extension additions
    (0L until count).foreach(_ => buf.readBit())
    maybeAlign(buf)
    (0L until count).foreach { _ =>
      val len = readLengthDeterminant(buf)
      buf.readByteArray(len.toInt)
    }
  }

  // "Normally small" length (X.691 §11.7): used for extension addition bitmaps.
  private def readNormallySmallLength(buf: PerBitBuffer): Long = {
    val small = buf.readBit()
    if (small == 0) {
      buf.readBits(6) + 1L
    } else {
      readLengthDeterminant(buf)
    }
  }

  private def berOidBytesToString(bytes: Array[Byte]): String = {
    if (bytes.isEmpty) return ""
    val sb = new StringBuilder
    var i = 0
    var value = 0L
    var first = true
    while (i < bytes.length) {
      val b = bytes(i) & 0xff
      value = (value << 7) | (b & 0x7f)
      if ((b & 0x80) == 0) {
        if (first) {
          sb.append(value / 40).append('.').append(value % 40)
          first = false
        } else {
          sb.append('.').append(value)
        }
        value = 0L
      }
      i += 1
    }
    sb.toString()
  }

  private def resolveRefs(t: Asn1Type): Asn1Type = t match {
    case ref: Asn1TypeReference =>
      val resolved = registry.resolveRef(ref, moduleName)
      if (resolved eq ref) ref else resolveRefs(resolved)
    case _ => t
  }
}
