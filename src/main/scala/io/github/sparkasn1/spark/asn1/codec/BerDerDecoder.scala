package io.github.sparkasn1.spark.asn1.codec

import io.github.sparkasn1.spark.asn1.model._
import io.github.sparkasn1.spark.asn1.parser.SchemaRegistry
import io.github.sparkasn1.spark.asn1.schema.Asn1TypeMapper
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.apache.spark.unsafe.types.UTF8String
import org.bouncycastle.asn1._

import java.io.InputStream

/**
 * Decodes ASN.1 BER/DER encoded messages into Spark InternalRow values.
 *
 * Decoding is schema-driven: the expected Asn1Type controls how each
 * TLV element is interpreted and converted to its Spark SQL value.
 *
 * Under AUTOMATIC / IMPLICIT TAGS modules each SEQUENCE component carries a
 * context-specific tag `[n]`.  The decoder inspects these tags to correctly
 * align present elements with their schema components even when OPTIONAL
 * fields are absent.
 */
class BerDerDecoder(registry: SchemaRegistry, moduleName: String) {

  /** Open a streaming parser over the given InputStream. */
  def openParser(is: InputStream): ASN1StreamParser = new ASN1StreamParser(is)

  /**
   * Read the next complete ASN.1 object from the stream and decode it against
   * `schema`.  Returns None when the stream is exhausted.
   */
  def decodeNext(
    parser:         ASN1StreamParser,
    schema:         Asn1Type,
    requiredFields: Option[Seq[String]] = None
  ): Option[InternalRow] = {
    try {
      val obj = parser.readObject()
      if (obj == null) None
      else Some(decodeToRow(obj.toASN1Primitive, schema, requiredFields))
    } catch {
      case _: java.io.EOFException => None
    }
  }

  /** Decode a raw DER/BER byte array against `schema`. Useful in tests. */
  def decodeBytes(bytes: Array[Byte], schema: Asn1Type): InternalRow = {
    val obj = ASN1Primitive.fromByteArray(bytes)
    decodeToRow(obj, schema)
  }

  // -----------------------------------------------------------------------
  // Core decode dispatch
  // -----------------------------------------------------------------------

  private def decodeToRow(
    obj:            ASN1Primitive,
    schema:         Asn1Type,
    requiredFields: Option[Seq[String]] = None
  ): InternalRow = {
    val eff = resolveRefs(schema)
    eff match {
      case s: Asn1Sequence    => decodeSequence(obj, s, requiredFields)
      case s: Asn1Set         => decodeSet(obj, s, requiredFields)
      case c: Asn1Choice      => decodeChoice(obj, c)
      case tt: Asn1TaggedType => new GenericInternalRow(Array[Any](decodeTaggedValue(obj, tt)))
      case _                  => new GenericInternalRow(Array[Any](decodeValue(obj, eff)))
    }
  }

  /** Decode one ASN.1 primitive to a Spark-internal value (not a Row). */
  def decodeValue(obj: ASN1Primitive, schema: Asn1Type): Any = {
    val eff = resolveRefs(schema)
    eff match {
      case Asn1Boolean          => decodeBoolean(obj)
      case Asn1Null             => null
      case _: Asn1Integer       => decodeInteger(obj)
      case Asn1OctetString      => decodeOctetString(obj)
      case Asn1ObjectIdentifier => decodeOid(obj)
      case Asn1RelativeOid      => decodeRelativeOid(obj)
      case _: Asn1StringType    => decodeString(obj)
      case e: Asn1Enumerated    => decodeEnumerated(obj, e)
      case b: Asn1BitString     => decodeBitString(obj, b)
      case s: Asn1Sequence      => decodeSequence(obj, s)
      case s: Asn1Set           => decodeSet(obj, s)
      case so: Asn1SequenceOf   => decodeSequenceOf(obj, so)
      case so: Asn1SetOf        => decodeSetOf(obj, so)
      case c: Asn1Choice        => decodeChoice(obj, c)
      case tt: Asn1TaggedType   => decodeTaggedValue(obj, tt)
      case Asn1Any              => obj.getEncoded
      case _                    => obj.getEncoded
    }
  }

  // -----------------------------------------------------------------------
  // Primitive decoders
  // -----------------------------------------------------------------------

  private def decodeBoolean(obj: ASN1Primitive): Boolean = obj match {
    case b: ASN1Boolean => b.isTrue
    case _              => false
  }

  private def decodeInteger(obj: ASN1Primitive): Long = obj match {
    case i: ASN1Integer =>
      try i.longValueExact()
      catch { case _: ArithmeticException => i.getValue.longValue() }
    case _ => 0L
  }

  private def decodeOctetString(obj: ASN1Primitive): Array[Byte] = obj match {
    case os: ASN1OctetString => os.getOctets
    case _                   => obj.getEncoded
  }

  private def decodeOid(obj: ASN1Primitive): UTF8String = obj match {
    case oid: ASN1ObjectIdentifier => UTF8String.fromString(oid.getId)
    case _                         => UTF8String.fromString(obj.toString)
  }

  private def decodeRelativeOid(obj: ASN1Primitive): UTF8String =
    UTF8String.fromString(obj.toString)

  private def decodeString(obj: ASN1Primitive): UTF8String = obj match {
    case s: ASN1String => UTF8String.fromString(s.getString)
    case _             => UTF8String.fromString(obj.toString)
  }

  private def decodeEnumerated(obj: ASN1Primitive, schema: Asn1Enumerated): UTF8String = {
    val intVal: Long = obj match {
      case e: ASN1Enumerated => e.getValue.longValue()
      case i: ASN1Integer    => i.longValueExact()
      case _                 => 0L
    }
    val name = schema.values.find(_.value.contains(intVal))
      .orElse(schema.values.find(_.value.isEmpty))
      .map(_.name)
      .getOrElse(intVal.toString)
    UTF8String.fromString(name)
  }

  private def decodeBitString(obj: ASN1Primitive, schema: Asn1BitString): Any = {
    val bytes: Array[Byte] = obj match {
      case bs: DERBitString => bs.getBytes
      case bs: BERBitString => bs.getBytes
      case _                => obj.getEncoded
    }
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

  // -----------------------------------------------------------------------
  // Constructed type decoders
  // -----------------------------------------------------------------------

  private def decodeSequence(
    obj:            ASN1Primitive,
    schema:         Asn1Sequence,
    requiredFields: Option[Seq[String]] = None
  ): InternalRow = {
    val elements = extractElements(obj, "SEQUENCE")
    val required = requiredFields.map(_.toSet).getOrElse(Set.empty)
    val allValues = decodeComponents(elements, schema.components, required)
    requiredFields match {
      case None => new GenericInternalRow(allValues)
      case Some(fields) =>
        val nameToIdx = schema.components.zipWithIndex.map { case (c, i) => c.name -> i }.toMap
        new GenericInternalRow(fields.map(f => allValues(nameToIdx(f))).toArray[Any])
    }
  }

  private def decodeSet(
    obj:            ASN1Primitive,
    schema:         Asn1Set,
    requiredFields: Option[Seq[String]] = None
  ): InternalRow = {
    val elements = extractElements(obj, "SET")
    val required = requiredFields.map(_.toSet).getOrElse(Set.empty)
    val allValues = decodeComponents(elements, schema.components, required)
    requiredFields match {
      case None => new GenericInternalRow(allValues)
      case Some(fields) =>
        val nameToIdx = schema.components.zipWithIndex.map { case (c, i) => c.name -> i }.toMap
        new GenericInternalRow(fields.map(f => allValues(nameToIdx(f))).toArray[Any])
    }
  }

  private def decodeSequenceOf(obj: ASN1Primitive, schema: Asn1SequenceOf): ArrayData = {
    val elements = extractElements(obj, "SEQUENCE OF")
    val elems    = elements.map {
      case t: ASN1TaggedObject => decodeTaggedField(t, schema.elementType)
      case e                   => decodeValue(e, schema.elementType)
    }
    new GenericArrayData(elems.toArray[Any])
  }

  private def decodeSetOf(obj: ASN1Primitive, schema: Asn1SetOf): ArrayData = {
    val elements = extractElements(obj, "SET OF")
    val elems    = elements.map {
      case t: ASN1TaggedObject => decodeTaggedField(t, schema.elementType)
      case e                   => decodeValue(e, schema.elementType)
    }
    new GenericArrayData(elems.toArray[Any])
  }

  /** Extract elements from a SEQUENCE or SET primitive as a list of ASN1Primitive. */
  private def extractElements(obj: ASN1Primitive, label: String): Seq[ASN1Primitive] =
    obj match {
      case s: ASN1Sequence => (0 until s.size()).map(i => s.getObjectAt(i).toASN1Primitive)
      case s: ASN1Set      => (0 until s.size()).map(i => s.getObjectAt(i).toASN1Primitive)
      case t: ASN1TaggedObject =>
        // Handle the case where the whole constructed value is wrapped in a tag
        val inner = t.getBaseObject.toASN1Primitive
        extractElements(inner, label)
      case _ =>
        throw new Asn1DecodeException(s"Expected $label, got ${obj.getClass.getSimpleName}")
    }

  /**
   * Decode components from a list of raw elements.
   *
   * Under AUTOMATIC / IMPLICIT tagging each element may be a DERTaggedObject
   * with a tag number corresponding to the field's index in the schema.
   * Absent OPTIONAL fields produce no element and leave their slot null.
   *
   * Fall-back (no tagging / explicit tagging / untagged module): align positionally.
   */
  private def decodeComponents(
    elements:      Seq[ASN1Primitive],
    components:    Seq[ComponentType],
    requiredNames: Set[String] = Set.empty   // empty = decode all
  ): Array[Any] = {
    val decodeAll = requiredNames.isEmpty
    val result    = new Array[Any](components.size)

    val useTagBasedAlignment = elements.nonEmpty && isContextTagged(elements.head)

    if (useTagBasedAlignment) {
      val tagMap: Map[Int, ASN1TaggedObject] = elements.flatMap {
        case t: ASN1TaggedObject => Some(t.getTagNo -> t)
        case _                   => None
      }.toMap
      components.zipWithIndex.foreach { case (comp, idx) =>
        if (decodeAll || requiredNames.contains(comp.name))
          result(idx) = tagMap.get(idx).map(t => decodeTaggedField(t, comp.asn1Type)).orNull
        // else leave result(idx) = null (slot unused after projection)
      }
    } else {
      var seqIdx = 0
      components.zipWithIndex.foreach { case (comp, fieldIdx) =>
        if (seqIdx < elements.size) {
          if (decodeAll || requiredNames.contains(comp.name))
            result(fieldIdx) = decodeValue(unwrapTagged(elements(seqIdx)), comp.asn1Type)
          seqIdx += 1
        }
        // else result(fieldIdx) stays null
      }
    }
    result
  }

  /**
   * Decode a context-tagged field element, handling both EXPLICIT and IMPLICIT tagging.
   * EXPLICIT: inner primitive retains its universal type tag — unwrap and dispatch normally.
   * IMPLICIT: original type tag replaced by context tag — reconstruct using the expected schema type.
   */
  private def decodeTaggedField(tagged: ASN1TaggedObject, schema: Asn1Type): Any =
    if (tagged.isExplicit)
      decodeValue(tagged.getBaseObject.toASN1Primitive, schema)
    else
      decodeValue(reinterpretImplicit(tagged, resolveRefs(schema)), schema)

  /**
   * Reconstruct a properly-typed ASN1Primitive from an IMPLICIT-tagged object.
   * Uses getBaseUniversal(false, universalTag) (BouncyCastle 1.69+) to re-stamp the
   * value bytes with the original universal tag, restoring the correct parsed type.
   */
  private def reinterpretImplicit(tagged: ASN1TaggedObject, schema: Asn1Type): ASN1Primitive = {
    val uTag = universalTagFor(schema)
    if (uTag < 0) tagged.getBaseObject.toASN1Primitive
    else try tagged.getBaseUniversal(false, uTag)
         catch { case _: Exception => tagged.getBaseObject.toASN1Primitive }
  }

  private def universalTagFor(schema: Asn1Type): Int = schema match {
    case Asn1Boolean           => BERTags.BOOLEAN
    case _: Asn1Integer        => BERTags.INTEGER
    case _: Asn1BitString      => BERTags.BIT_STRING
    case Asn1OctetString       => BERTags.OCTET_STRING
    case Asn1Null              => BERTags.NULL
    case Asn1ObjectIdentifier  => BERTags.OBJECT_IDENTIFIER
    case Asn1RelativeOid       => BERTags.RELATIVE_OID
    case Asn1Utf8String        => BERTags.UTF8_STRING
    case Asn1PrintableString   => BERTags.PRINTABLE_STRING
    case Asn1Ia5String         => BERTags.IA5_STRING
    case Asn1VisibleString     => BERTags.VISIBLE_STRING
    case Asn1NumericString     => BERTags.NUMERIC_STRING
    case Asn1GeneralString     => BERTags.GENERAL_STRING
    case Asn1BmpString         => BERTags.BMP_STRING
    case _: Asn1StringType     => BERTags.UTF8_STRING
    case _: Asn1Enumerated     => BERTags.ENUMERATED
    case _: Asn1Sequence       => BERTags.SEQUENCE
    case _: Asn1SequenceOf     => BERTags.SEQUENCE
    case _: Asn1Set            => BERTags.SET
    case _: Asn1SetOf          => BERTags.SET
    case _                     => -1
  }

  private def isContextTagged(obj: ASN1Primitive): Boolean = obj match {
    case t: ASN1TaggedObject =>
      t.getTagClass == BERTags.CONTEXT_SPECIFIC
    case _ => false
  }

  /** Strip an outer explicit or implicit context tag, returning the inner primitive. */
  private def unwrapTagged(obj: ASN1Primitive): ASN1Primitive = obj match {
    case t: ASN1TaggedObject => t.getBaseObject.toASN1Primitive
    case _                   => obj
  }

  // -----------------------------------------------------------------------
  // CHOICE decoder
  // -----------------------------------------------------------------------

  private def decodeChoice(obj: ASN1Primitive, schema: Asn1Choice): InternalRow = {
    val (altIdx, altValue) = matchChoiceAlternative(obj, schema)
    val fields             = new Array[Any](1 + schema.alternatives.size)
    val altName            = if (altIdx >= 0) schema.alternatives(altIdx).name else "unknown"
    fields(0)              = UTF8String.fromString(altName)
    if (altIdx >= 0) fields(altIdx + 1) = altValue
    new GenericInternalRow(fields)
  }

  private def matchChoiceAlternative(
    obj: ASN1Primitive,
    schema: Asn1Choice
  ): (Int, Any) = {
    obj match {
      case tagged: ASN1TaggedObject if tagged.getTagClass == BERTags.CONTEXT_SPECIFIC =>
        val tagNo  = tagged.getTagNo
        val altIdx = if (tagNo < schema.alternatives.size) tagNo else -1
        if (altIdx >= 0) {
          val alt   = schema.alternatives(altIdx)
          val inner = tagged.getBaseObject.toASN1Primitive
          (altIdx, decodeValue(inner, alt.asn1Type))
        } else (-1, null)

      case _ =>
        // Untagged CHOICE: match by universal tag class
        val matched = schema.alternatives.zipWithIndex.find { case (alt, _) =>
          universalTagMatches(obj, resolveRefs(alt.asn1Type))
        }
        matched match {
          case Some((alt, idx)) => (idx, decodeValue(obj, alt.asn1Type))
          case None             => (-1, null)
        }
    }
  }

  private def universalTagMatches(obj: ASN1Primitive, schema: Asn1Type): Boolean =
    schema match {
      case Asn1Boolean          => obj.isInstanceOf[ASN1Boolean]
      case _: Asn1Integer       => obj.isInstanceOf[ASN1Integer]
      case Asn1OctetString      => obj.isInstanceOf[ASN1OctetString]
      case _: Asn1BitString     => obj.isInstanceOf[DERBitString] || obj.isInstanceOf[BERBitString]
      case Asn1ObjectIdentifier => obj.isInstanceOf[ASN1ObjectIdentifier]
      case _: Asn1StringType    => obj.isInstanceOf[ASN1String]
      case _: Asn1Enumerated    => obj.isInstanceOf[ASN1Enumerated]
      case _: Asn1Sequence      => obj.isInstanceOf[ASN1Sequence]
      case _: Asn1Set           => obj.isInstanceOf[ASN1Set]
      case _: Asn1SequenceOf    => obj.isInstanceOf[ASN1Sequence]
      case _                    => false
    }

  private def decodeTaggedValue(obj: ASN1Primitive, tt: Asn1TaggedType): Any = {
    val inner: ASN1Primitive = obj match {
      case t: ASN1TaggedObject => t.getBaseObject.toASN1Primitive
      case _                   => obj
    }
    decodeValue(inner, tt.innerType)
  }

  // -----------------------------------------------------------------------
  // Reference resolution
  // -----------------------------------------------------------------------

  private def resolveRefs(t: Asn1Type): Asn1Type = t match {
    case ref: Asn1TypeReference =>
      val resolved = registry.resolveRef(ref, moduleName)
      if (resolved == ref) ref else resolveRefs(resolved)
    case _ => t
  }
}

class Asn1DecodeException(msg: String, cause: Throwable = null)
  extends RuntimeException(msg, cause)
