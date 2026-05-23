package io.github.sparkasn1.spark.asn1.codec

import io.github.sparkasn1.spark.asn1.model._
import io.github.sparkasn1.spark.asn1.parser.SchemaRegistry
import io.github.sparkasn1.spark.asn1.schema.Asn1TypeMapper
import io.github.sparkasn1.spark.asn1.util.BerRealUtil
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String
import org.bouncycastle.asn1._

import java.io.{ByteArrayOutputStream, InputStream}

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
class BerDerDecoder(
  registry:        SchemaRegistry,
  moduleName:      String,
  enumeratedAsInt: Boolean = false
) {

  /**
   * Return the InputStream to use for streaming. Retained for API symmetry with
   * decodeNext; the caller passes this value back to decodeNext each time.
   */
  def openParser(is: InputStream): InputStream = is

  /**
   * Read the next complete ASN.1 object from the stream and decode it against
   * `schema`.  Returns None when the stream is exhausted.
   *
   * Reads one raw TLV frame before handing bytes to BouncyCastle so that REAL
   * elements (tag 9, unknown to BC 1.80) can be masked in-place before parsing.
   */
  def decodeNext(
    is:             InputStream,
    schema:         Asn1Type,
    requiredSchema: Option[StructType] = None
  ): Option[InternalRow] = {
    readRawTlv(is) match {
      case None => None
      case Some(rawBytes) =>
        val masked = maskRealBytes(rawBytes)
        val obj    = ASN1Primitive.fromByteArray(masked)
        Some(decodeToRow(obj, schema, requiredSchema))
    }
  }

  /**
   * Decode a raw DER/BER byte array against `schema`.
   * REAL elements (tag 9) are masked as OCTET STRING before BouncyCastle parsing.
   */
  def decodeBytes(bytes: Array[Byte], schema: Asn1Type): InternalRow = {
    val masked = maskRealBytes(bytes)
    val obj    = ASN1Primitive.fromByteArray(masked)
    decodeToRow(obj, schema)
  }

  // -----------------------------------------------------------------------
  // Core decode dispatch
  // -----------------------------------------------------------------------

  private def decodeToRow(
    obj:            ASN1Primitive,
    schema:         Asn1Type,
    requiredSchema: Option[StructType] = None
  ): InternalRow = {
    val eff = resolveRefs(schema)
    eff match {
      case s: Asn1Sequence    => decodeSequence(obj, s, requiredSchema)
      case s: Asn1Set         => decodeSet(obj, s, requiredSchema)
      case c: Asn1Choice      => decodeChoice(obj, c, requiredSchema)
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
      case Asn1Real =>
        val content = obj match {
          case os: ASN1OctetString => os.getOctets  // masked REAL (09→04 before BC parsing)
          case _                   => extractRealContent(obj)
        }
        BerRealUtil.decodeContent(content)
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

  private def decodeEnumerated(obj: ASN1Primitive, schema: Asn1Enumerated): Any = {
    val intVal: Long = obj match {
      case e: ASN1Enumerated => e.getValue.longValue()
      case i: ASN1Integer    => i.longValueExact()
      case _                 => 0L
    }
    if (enumeratedAsInt) intVal
    else {
      val name = schema.values.find(_.value.contains(intVal))
        .orElse(schema.values.find(_.value.isEmpty))
        .map(_.name)
        .getOrElse(intVal.toString)
      UTF8String.fromString(name)
    }
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
    requiredSchema: Option[StructType] = None
  ): InternalRow = {
    val elements  = extractElements(obj, "SEQUENCE")
    val required  = requiredSchema.map(st => st.fieldNames.toSet).getOrElse(Set.empty)
    val allValues = decodeComponents(elements, schema.components, required, requiredSchema)
    requiredSchema match {
      case None => new GenericInternalRow(allValues)
      case Some(st) =>
        val nameToIdx = schema.components.zipWithIndex.map { case (c, i) => c.name -> i }.toMap
        new GenericInternalRow(st.fieldNames.map(f => allValues(nameToIdx(f))).toArray[Any])
    }
  }

  private def decodeSet(
    obj:            ASN1Primitive,
    schema:         Asn1Set,
    requiredSchema: Option[StructType] = None
  ): InternalRow = {
    val elements  = extractElements(obj, "SET")
    val required  = requiredSchema.map(st => st.fieldNames.toSet).getOrElse(Set.empty)
    val allValues = decodeComponents(elements, schema.components, required, requiredSchema)
    requiredSchema match {
      case None => new GenericInternalRow(allValues)
      case Some(st) =>
        val nameToIdx = schema.components.zipWithIndex.map { case (c, i) => c.name -> i }.toMap
        new GenericInternalRow(st.fieldNames.map(f => allValues(nameToIdx(f))).toArray[Any])
    }
  }

  private def decodeSequenceOf(obj: ASN1Primitive, schema: Asn1SequenceOf): ArrayData = {
    val elemResolved = resolveRefs(schema.elementType)
    if (elemResolved == Asn1Real)
      return new GenericArrayData(extractRealTlvs(obj).map(BerRealUtil.decodeContent).toArray[Any])
    val elements = extractElements(obj, "SEQUENCE OF")
    val isChoice = elemResolved.isInstanceOf[Asn1Choice]
    val elems = elements.map {
      case t: ASN1TaggedObject if isChoice => decodeValue(t, schema.elementType)
      case t: ASN1TaggedObject             => decodeTaggedField(t, schema.elementType)
      case e                               => decodeValue(e, schema.elementType)
    }
    new GenericArrayData(elems.toArray[Any])
  }

  private def decodeSetOf(obj: ASN1Primitive, schema: Asn1SetOf): ArrayData = {
    val elemResolved = resolveRefs(schema.elementType)
    if (elemResolved == Asn1Real)
      return new GenericArrayData(extractRealTlvs(obj).map(BerRealUtil.decodeContent).toArray[Any])
    val elements = extractElements(obj, "SET OF")
    val isChoice = elemResolved.isInstanceOf[Asn1Choice]
    val elems = elements.map {
      case t: ASN1TaggedObject if isChoice => decodeValue(t, schema.elementType)
      case t: ASN1TaggedObject             => decodeTaggedField(t, schema.elementType)
      case e                               => decodeValue(e, schema.elementType)
    }
    new GenericArrayData(elems.toArray[Any])
  }

  private def parseBerTlvContents(bytes: Array[Byte]): Seq[Array[Byte]] = {
    val result = scala.collection.mutable.ArrayBuffer.empty[Array[Byte]]
    var i = 0
    while (i < bytes.length) {
      if (i + 1 >= bytes.length) i = bytes.length
      else {
        val lenByte = bytes(i + 1) & 0xff
        val (headerLen, elemLen) =
          if (lenByte <= 127) (2, lenByte)
          else {
            val numLen = lenByte & 0x7f
            val cLen   = (0 until numLen).foldLeft(0)((acc, j) =>
              (acc << 8) | (if (i + 2 + j < bytes.length) bytes(i + 2 + j) & 0xff else 0))
            (2 + numLen, cLen)
          }
        result += bytes.slice(i + headerLen, i + headerLen + elemLen)
        i += headerLen + elemLen
      }
    }
    result.toSeq
  }

  private def extractRealTlvs(obj: ASN1Primitive): Seq[Array[Byte]] =
    parseBerTlvContents(stripTagLen(obj.getEncoded("DER")))

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
   * Decode components from a list of raw elements using one of three strategies:
   *
   * 1. Hybrid (schema has explicit tagged-type declarations):
   *    Components with Asn1TaggedType (APPLICATION or CONTEXT) are matched by
   *    (tagClass, tagNumber); bare components are matched positionally.
   *    Covers: EXPLICIT/IMPLICIT TAGS modules (RFC 5280 X.509), APPLICATION tags
   *    (telecom CDR), and mixed schemas.
   *
   * 2. Index-based (AUTOMATIC TAGS style):
   *    No schema-level tagged types, but wire elements are all context-tagged.
   *    Tag number equals the component's schema index.
   *
   * 3. Positional:
   *    No context tags anywhere; components align one-to-one in order.
   */
  private def decodeComponents(
    elements:      Seq[ASN1Primitive],
    components:    Seq[ComponentType],
    requiredNames: Set[String]         = Set.empty,
    parentSchema:  Option[StructType]  = None
  ): Array[Any] = {
    val decodeAll = requiredNames.isEmpty
    val result    = new Array[Any](components.size)

    val hasSchemaTaggedTypes = components.exists { c =>
      resolveRefs(c.asn1Type).isInstanceOf[Asn1TaggedType]
    }

    if (hasSchemaTaggedTypes) {
      // Hybrid: tagged components matched by (tagClass, tagNo), bare types positionally.
      val wireTagged: Map[(Int, Int), ASN1TaggedObject] = elements.flatMap {
        case t: ASN1TaggedObject => Some((t.getTagClass, t.getTagNo) -> t)
        case _                   => None
      }.toMap
      val wireUntagged = scala.collection.mutable.Queue(
        elements.filterNot(_.isInstanceOf[ASN1TaggedObject]): _*)

      components.zipWithIndex.foreach { case (comp, idx) =>
        resolveRefs(comp.asn1Type) match {
          case tt: Asn1TaggedType =>
            val berClass = tt.tagClass match {
              case TagClass.ContextSpecific => BERTags.CONTEXT_SPECIFIC
              case TagClass.Application     => BERTags.APPLICATION
              case TagClass.Private         => BERTags.PRIVATE
              case TagClass.Universal       => BERTags.UNIVERSAL
            }
            if (decodeAll || requiredNames.contains(comp.name)) {
              val inner = resolveRefs(tt.innerType)
              val child = childPruningFor(comp.name, parentSchema)
              result(idx) = wireTagged.get((berClass, tt.tagNumber)).map { t =>
                val obj = tt.tagging match {
                  case Tagging.Explicit => t.getBaseObject.toASN1Primitive
                  case _                => reinterpretImplicit(t, inner)
                }
                decodeValuePruned(obj, inner, child)
              }.orNull
            }
          case _ =>
            if (wireUntagged.nonEmpty) {
              val elem = wireUntagged.dequeue()
              if (decodeAll || requiredNames.contains(comp.name)) {
                val child = childPruningFor(comp.name, parentSchema)
                result(idx) = decodeValuePruned(elem, comp.asn1Type, child)
              }
            }
        }
      }
    } else if (elements.nonEmpty && isContextTagged(elements.head)) {
      // Index-based: AUTOMATIC TAGS style — tag number equals field index.
      val tagMap: Map[Int, ASN1TaggedObject] = elements.flatMap {
        case t: ASN1TaggedObject => Some(t.getTagNo -> t)
        case _                   => None
      }.toMap
      components.zipWithIndex.foreach { case (comp, idx) =>
        if (decodeAll || requiredNames.contains(comp.name)) {
          val child = childPruningFor(comp.name, parentSchema)
          result(idx) = tagMap.get(idx).map(t => decodeTaggedField(t, comp.asn1Type, child)).orNull
        }
      }
    } else {
      // Positional: no context tags anywhere.
      var seqIdx = 0
      components.zipWithIndex.foreach { case (comp, fieldIdx) =>
        if (seqIdx < elements.size) {
          if (decodeAll || requiredNames.contains(comp.name)) {
            val child = childPruningFor(comp.name, parentSchema)
            result(fieldIdx) = decodeValuePruned(unwrapTagged(elements(seqIdx)), comp.asn1Type, child)
          }
          seqIdx += 1
        }
      }
    }
    result
  }

  /**
   * Decode a value, propagating deep schema pruning into nested SEQUENCE/SET types.
   * For all other types, delegates to the standard decodeValue.
   */
  private def decodeValuePruned(
    obj:          ASN1Primitive,
    schema:       Asn1Type,
    childPruning: Option[StructType]
  ): Any = childPruning match {
    case None => decodeValue(obj, schema)
    case Some(st) =>
      resolveRefs(schema) match {
        case s: Asn1Sequence => decodeSequence(obj, s, Some(st))
        case s: Asn1Set      => decodeSet(obj, s, Some(st))
        case _               => decodeValue(obj, schema)
      }
  }

  /** Extract the child StructType for a named field from a parent required schema. */
  private def childPruningFor(compName: String, parentSchema: Option[StructType]): Option[StructType] =
    parentSchema.flatMap { st =>
      st.fields.find(_.name == compName).map(_.dataType).collect {
        case childSt: StructType => childSt
      }
    }

  /**
   * Decode a tagged field from the index-based path (AUTOMATIC TAGS).
   *
   * For primitive inner types: use BouncyCastle's isExplicit() to distinguish
   * EXPLICIT (inner has its own universal TLV) from IMPLICIT (tag replaces universal tag).
   *
   * For constructed inner types (SEQUENCE, SET, …): AUTOMATIC TAGS always uses IMPLICIT
   * tagging (tag replaces the universal constructed tag), but BouncyCastle marks the outer
   * tag as explicit=true when the content itself looks structured.  Force the IMPLICIT path.
   */
  private def decodeTaggedField(
    tagged:       ASN1TaggedObject,
    schema:       Asn1Type,
    childPruning: Option[StructType] = None
  ): Any = {
    val eff = resolveRefs(schema)
    eff match {
      case Asn1Real =>
        // BouncyCastle 1.80 cannot parse REAL (tag 9); extract content from the tagged encoding.
        BerRealUtil.decodeContent(extractRealContent(tagged))

      case _: Asn1Choice =>
        // CHOICE fields in AUTOMATIC TAGS modules use EXPLICIT outer tags. The inner content
        // is a context-tagged alternative (not a raw universal TLV), so toASN1Primitive is safe.
        if (tagged.isExplicit)
          decodeChoice(tagged.getBaseObject.toASN1Primitive, eff.asInstanceOf[Asn1Choice])
        else
          decodeValue(reinterpretImplicit(tagged, eff), eff)

      case so: Asn1SequenceOf if resolveRefs(so.elementType) == Asn1Real =>
        // getBaseUniversal fails for SEQUENCE OF REAL because BouncyCastle tries to eagerly
        // parse REAL sub-elements (tag 9). Extract the inner content bytes directly.
        new GenericArrayData(
          parseBerTlvContents(stripTagLen(tagged.getEncoded("DER")))
            .map(BerRealUtil.decodeContent).toArray[Any])

      case so: Asn1SetOf if resolveRefs(so.elementType) == Asn1Real =>
        new GenericArrayData(
          parseBerTlvContents(stripTagLen(tagged.getEncoded("DER")))
            .map(BerRealUtil.decodeContent).toArray[Any])

      case _ =>
        val implicitForConstructed = eff match {
          case _: Asn1Sequence | _: Asn1SequenceOf | _: Asn1Set | _: Asn1SetOf => true
          case _ => false
        }
        val inner =
          if (!tagged.isExplicit || implicitForConstructed) reinterpretImplicit(tagged, eff)
          else tagged.getBaseObject.toASN1Primitive
        decodeValuePruned(inner, eff, childPruning)
    }
  }

  private def stripTagLen(enc: Array[Byte]): Array[Byte] = {
    if (enc.length < 2) return Array.empty
    val lenByte = enc(1) & 0xff
    val headerLen =
      if (lenByte <= 127) 2
      else 2 + (lenByte & 0x7f)
    if (headerLen >= enc.length) Array.empty
    else enc.slice(headerLen, enc.length)
  }

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

  /** Extract BER REAL content bytes from any ASN1 object (universal or implicit-tagged). */
  private def extractRealContent(obj: ASN1Primitive): Array[Byte] =
    extractRealContent(obj.getEncoded("DER"), expectTag9 = true)

  private def extractRealContent(tagged: ASN1TaggedObject): Array[Byte] =
    extractRealContent(tagged.getEncoded("DER"), expectTag9 = false)

  private def extractRealContent(enc: Array[Byte], expectTag9: Boolean): Array[Byte] = {
    if (enc.length < 2) return Array.empty
    if (expectTag9 && (enc(0) & 0xff) != 0x09) return Array.empty
    val lenByte = enc(1) & 0xff
    val (contentStart, contentLen) =
      if (lenByte <= 127) (2, lenByte)
      else {
        val numLen = lenByte & 0x7f
        val cLen   = (0 until numLen).foldLeft(0)((acc, i) => (acc << 8) | (enc(2 + i) & 0xff))
        (2 + numLen, cLen)
      }
    enc.slice(contentStart, contentStart + contentLen)
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

  private def decodeChoice(
    obj:            ASN1Primitive,
    schema:         Asn1Choice,
    requiredSchema: Option[StructType] = None
  ): InternalRow = {
    val (altIdx, altValue) = matchChoiceAlternative(obj, schema)
    val altName            = if (altIdx >= 0) schema.alternatives(altIdx).name else "unknown"
    requiredSchema match {
      case None =>
        val fields = new Array[Any](1 + schema.alternatives.size)
        fields(0) = UTF8String.fromString(altName)
        if (altIdx >= 0) fields(altIdx + 1) = altValue
        new GenericInternalRow(fields)
      case Some(st) =>
        // The required struct contains the _tag field plus whichever alternatives are needed.
        // Fields that are not alternative names are the discriminator field (e.g. "_tag").
        val altNames = schema.alternatives.map(_.name).toSet
        val result = st.fieldNames.map { n =>
          if (!altNames.contains(n)) {
            UTF8String.fromString(altName)         // discriminator field (_tag or renamed)
          } else {
            val altI = schema.alternatives.indexWhere(_.name == n)
            if (altI >= 0 && altI == altIdx) altValue else null
          }
        }
        new GenericInternalRow(result.toArray[Any])
    }
  }

  private def matchChoiceAlternative(
    obj: ASN1Primitive,
    schema: Asn1Choice
  ): (Int, Any) = {
    obj match {
      case tagged: ASN1TaggedObject =>
        val wireClass = tagged.getTagClass
        val wireTagNo = tagged.getTagNo

        // 1. Schema-tagged alternatives: find by (tagClass, tagNo)
        val schemaTagged = schema.alternatives.zipWithIndex.find { case (alt, _) =>
          resolveRefs(alt.asn1Type) match {
            case tt: Asn1TaggedType =>
              val berClass = tt.tagClass match {
                case TagClass.ContextSpecific => BERTags.CONTEXT_SPECIFIC
                case TagClass.Application     => BERTags.APPLICATION
                case TagClass.Private         => BERTags.PRIVATE
                case TagClass.Universal       => BERTags.UNIVERSAL
              }
              berClass == wireClass && tt.tagNumber == wireTagNo
            case _ => false
          }
        }

        schemaTagged match {
          case Some((alt, idx)) =>
            val tt    = resolveRefs(alt.asn1Type).asInstanceOf[Asn1TaggedType]
            val inner = resolveRefs(tt.innerType)
            val implicitForConstructed = inner match {
              case _: Asn1Sequence | _: Asn1SequenceOf | _: Asn1Set | _: Asn1SetOf => true
              case _ => false
            }
            val value = tt.tagging match {
              case Tagging.Explicit if !implicitForConstructed =>
                decodeValue(tagged.getBaseObject.toASN1Primitive, inner)
              case _ =>
                decodeValue(reinterpretImplicit(tagged, inner), inner)
            }
            (idx, value)

          case None if wireClass == BERTags.CONTEXT_SPECIFIC =>
            // 2. Fallback: index-based for AUTOMATIC TAGS (tagNo == altIndex)
            val altIdx = if (wireTagNo < schema.alternatives.size) wireTagNo else -1
            if (altIdx >= 0) {
              val alt         = schema.alternatives(altIdx)
              val altResolved = resolveRefs(alt.asn1Type)
              val value = altResolved match {
                case Asn1Real =>
                  BerRealUtil.decodeContent(extractRealContent(tagged))
                case _: Asn1Choice | _: Asn1Sequence | _: Asn1Set |
                     _: Asn1SequenceOf | _: Asn1SetOf =>
                  decodeValue(tagged, alt.asn1Type)
                case _ =>
                  decodeValue(reinterpretImplicit(tagged, altResolved), altResolved)
              }
              (altIdx, value)
            } else (-1, null)

          case None => (-1, null)
        }

      case _ =>
        // Untagged CHOICE: match by universal tag or recursive inner-type check
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
      // A CHOICE or TaggedType matches if any of its inner alternatives can match
      case c: Asn1Choice        => c.alternatives.exists(a => universalTagMatches(obj, resolveRefs(a.asn1Type)))
      case tt: Asn1TaggedType   => obj match {
        case t: ASN1TaggedObject =>
          val berClass = tt.tagClass match {
            case TagClass.ContextSpecific => BERTags.CONTEXT_SPECIFIC
            case TagClass.Application     => BERTags.APPLICATION
            case TagClass.Private         => BERTags.PRIVATE
            case TagClass.Universal       => BERTags.UNIVERSAL
          }
          berClass == t.getTagClass && tt.tagNumber == t.getTagNo
        case _ => false
      }
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
  // Raw TLV frame reader (used by decodeNext to capture bytes before parsing)
  // -----------------------------------------------------------------------

  /**
   * Read one complete BER/DER TLV from `is` and return its raw bytes.
   * Returns None at a clean EOF before the first byte of a new record.
   * Handles both definite-length and indefinite-length (depth-tracked) encoding.
   */
  private def readRawTlv(is: InputStream): Option[Array[Byte]] = {
    val out = new ByteArrayOutputStream(512)
    val t0  = is.read()
    if (t0 < 0) return None
    out.write(t0)
    if ((t0 & 0x1f) == 0x1f) {            // long-form tag: read continuation bytes
      var b = is.read()
      while (b >= 0 && (b & 0x80) != 0) { out.write(b); b = is.read() }
      if (b < 0) return None
      out.write(b)
    }
    val l0 = is.read()
    if (l0 < 0) return None
    out.write(l0)
    (l0 & 0xff) match {
      case 0x80 =>
        readIndefiniteContent(is, out)   // stops after matching EOC
      case n if n <= 127 =>
        rawReadFully(is, n, out)
      case n =>
        val nb  = n & 0x7f
        var len = 0
        (0 until nb).foreach { _ =>
          val b = is.read(); if (b < 0) return None; out.write(b)
          len = (len << 8) | (b & 0xff)
        }
        rawReadFully(is, len, out)
    }
    Some(out.toByteArray)
  }

  /** Read `len` bytes from `is` into `out`. */
  private def rawReadFully(is: InputStream, len: Int, out: ByteArrayOutputStream): Unit = {
    if (len <= 0) return
    val buf = new Array[Byte](math.min(len, 65536))
    var rem = len
    while (rem > 0) {
      val n = is.read(buf, 0, math.min(rem, buf.length))
      if (n < 0) return
      out.write(buf, 0, n)
      rem -= n
    }
  }

  /**
   * Read TLVs into `out` until the indefinite-length EOC (`00 00`) that matches
   * the current depth.  Handles nested indefinite-length constructions.
   */
  private def readIndefiniteContent(is: InputStream, out: ByteArrayOutputStream): Unit = {
    var depth = 1
    while (depth > 0) {
      val t = is.read(); if (t < 0) return; out.write(t)
      if ((t & 0x1f) == 0x1f) {           // long-form tag in child
        var b = is.read()
        while (b >= 0 && (b & 0x80) != 0) { out.write(b); b = is.read() }
        if (b < 0) return; out.write(b)
      }
      val l = is.read(); if (l < 0) return; out.write(l)
      (t, l & 0xff) match {
        case (0, 0)    => depth -= 1             // EOC
        case (_, 0x80) => depth += 1             // nested indefinite
        case (_, n) if n <= 127 => rawReadFully(is, n, out)
        case (_, n) =>
          val nb  = n & 0x7f
          var len = 0
          (0 until nb).foreach { _ =>
            val b = is.read(); if (b < 0) return; out.write(b)
            len = (len << 8) | (b & 0xff)
          }
          rawReadFully(is, len, out)
      }
    }
  }

  // -----------------------------------------------------------------------
  // REAL byte masking — replace tag 09 with 04 before BouncyCastle parsing
  // -----------------------------------------------------------------------

  /**
   * Return a copy of `bytes` with every REAL tag byte (0x09) replaced by an
   * OCTET STRING tag (0x04).  Recurses into all constructed TLVs so that REAL
   * elements nested inside SEQUENCE OF or CHOICE are also patched.
   *
   * decodeValue handles the masked OCTET STRING for Asn1Real by reading
   * os.getOctets() directly as the REAL content bytes.
   */
  private def maskRealBytes(bytes: Array[Byte]): Array[Byte] = {
    val result = bytes.clone()
    maskRealAt(result, 0, result.length)
    result
  }

  private def maskRealAt(b: Array[Byte], pos: Int, end: Int): Unit = {
    var i = pos
    while (i < end && i < b.length) {
      val tag = b(i) & 0xff

      // For long-form tags (bits 4-0 == 0x1f) the actual tag number is encoded in
      // one or more continuation bytes that follow, each with bit 7 set except the
      // last.  We must skip all of them to reach the actual length byte.
      var lenPos = i + 1
      if ((tag & 0x1f) == 0x1f) {
        while (lenPos < b.length && (b(lenPos) & 0x80) != 0) lenPos += 1
        lenPos += 1  // skip the last continuation byte (high bit clear)
      }
      if (lenPos >= b.length) return

      val lenByte = b(lenPos) & 0xff
      val tagLen  = lenPos - i       // bytes consumed by the tag field
      val (extraLen, contLen): (Int, Int) =
        if (lenByte <= 127) (0, lenByte)
        else {
          val nb   = lenByte & 0x7f
          var cLen = 0
          var j    = 0
          while (j < nb && lenPos + 1 + j < b.length) {
            cLen = (cLen << 8) | (b(lenPos + 1 + j) & 0xff)
            j += 1
          }
          (nb, cLen)
        }
      val hdrLen = tagLen + 1 + extraLen

      if (tag == 0x09) {
        b(i) = 0x04.toByte              // REAL → OCTET STRING (always single-byte tag)
      } else if ((tag & 0x20) != 0) {   // constructed: recurse into content
        maskRealAt(b, i + hdrLen, i + hdrLen + contLen)
      }
      val advance = hdrLen + contLen
      if (advance <= 0) return           // safety guard against malformed data
      i += advance
    }
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
