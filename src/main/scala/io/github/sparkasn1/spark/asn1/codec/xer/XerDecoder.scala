package io.github.sparkasn1.spark.asn1.codec.xer

import io.github.sparkasn1.spark.asn1.model._
import io.github.sparkasn1.spark.asn1.parser.SchemaRegistry
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.unsafe.types.UTF8String

import javax.xml.stream.XMLInputFactory
import javax.xml.stream.events.{EndElement, StartElement, XMLEvent}
import java.io.StringReader

/**
 * Schema-driven XER decoder using StAX pull parsing.
 *
 * Both Basic XER (BXER) and Canonical XER (CXER) are supported — the only
 * difference visible at the record level is whitespace handling, which StAX
 * normalises for us.
 *
 * Mapping summary (X.693):
 *  - NULL              → empty element, produces null
 *  - BOOLEAN           → <true/> or <false/>
 *  - INTEGER           → element text content parsed as Long
 *  - ENUMERATED        → element text is the symbolic name
 *  - OCTET STRING      → hex-encoded text (pairs of hex digits)
 *  - BIT STRING (unnamed) → hex-encoded text of the padded bytes
 *  - BIT STRING (named)   → named-bit list struct (_bytes + _namedBits)
 *  - String types      → element text content
 *  - OBJECT IDENTIFIER → dot-notation text (e.g. "1.2.840.10045.2.1")
 *  - SEQUENCE / SET    → child elements keyed by field name
 *  - SEQUENCE OF / SET OF → repeated child elements of any tag
 *  - CHOICE            → single child element whose tag names the active alternative
 *  - Tagged types      → inner type decoded (tags invisible in XER)
 */
class XerDecoder(
  registry:        SchemaRegistry,
  moduleName:      String,
  enumeratedAsInt: Boolean = false
) {

  private val factory: XMLInputFactory = {
    val f = XMLInputFactory.newInstance()
    f.setProperty(XMLInputFactory.IS_COALESCING, true)          // merge CHARACTERS events
    f.setProperty(XMLInputFactory.IS_NAMESPACE_AWARE, false)
    f
  }

  /**
   * Decode one XER record supplied as an XML string (the full element, e.g.
   * `<MyMessage>...</MyMessage>`).
   */
  def decodeXml(xml: String, schema: Asn1Type): InternalRow = {
    val reader = factory.createXMLEventReader(new StringReader(xml))
    // Skip to the first StartElement (the root element)
    while (reader.hasNext && !reader.peek().isStartElement) reader.nextEvent()
    if (!reader.hasNext)
      throw new XerDecodeException("Empty XER document")
    val startEvt = reader.nextEvent().asStartElement()
    val result   = decodeElement(reader, startEvt, resolveRefs(schema))
    result match {
      case row: InternalRow => row
      case other            => new GenericInternalRow(Array[Any](other))
    }
  }

  // -------------------------------------------------------------------------
  // Core dispatch — called after the opening StartElement has been consumed
  // -------------------------------------------------------------------------

  private def decodeElement(
    reader: javax.xml.stream.XMLEventReader,
    start: StartElement,
    schema: Asn1Type
  ): Any = schema match {
    case Asn1Null              => consumeToEnd(reader, start); null
    case Asn1Boolean           => decodeBoolean(reader, start)
    case _: Asn1Integer        => decodeText(reader, start).trim.toLong
    case Asn1OctetString       => hexStringToBytes(decodeText(reader, start).trim)
    case b: Asn1BitString      => decodeBitStringElement(reader, start, b)
    case Asn1ObjectIdentifier  => UTF8String.fromString(decodeText(reader, start).trim)
    case Asn1RelativeOid       => UTF8String.fromString(decodeText(reader, start).trim)
    case _: Asn1StringType     => UTF8String.fromString(decodeText(reader, start))
    case e: Asn1Enumerated     => decodeEnumeratedElement(reader, start, e)
    case s: Asn1Sequence       => decodeSequenceElement(reader, start, s)
    case s: Asn1Set            => decodeSequenceElement(reader, start, Asn1Sequence(s.components, s.extensible))
    case so: Asn1SequenceOf    => decodeSequenceOfElement(reader, start, so)
    case so: Asn1SetOf         => decodeSequenceOfElement(reader, start, Asn1SequenceOf(so.elementType, so.sizeConstraint))
    case c: Asn1Choice         => decodeChoiceElement(reader, start, c)
    case tt: Asn1TaggedType    => decodeElement(reader, start, resolveRefs(tt.innerType))
    case Asn1Any               => val t = decodeText(reader, start); UTF8String.fromString(t)
    case _                     => consumeToEnd(reader, start); null
  }

  // -------------------------------------------------------------------------
  // Primitive decoders
  // -------------------------------------------------------------------------

  private def decodeBoolean(reader: javax.xml.stream.XMLEventReader, start: StartElement): Boolean = {
    // XER BOOLEAN: container element holds a single <true/> or <false/> child.
    var boolName = "false"
    var done = false
    while (reader.hasNext && !done) {
      val evt = reader.nextEvent()
      if (evt.isStartElement) {
        boolName = evt.asStartElement().getName.getLocalPart
        consumeUntilEndElement(reader, 1)
      } else if (evt.isEndElement) {
        done = true
      }
    }
    boolName == "true"
  }

  private def decodeText(reader: javax.xml.stream.XMLEventReader, start: StartElement): String = {
    val sb = new StringBuilder
    var done = false
    while (reader.hasNext && !done) {
      val evt = reader.nextEvent()
      if (evt.isCharacters) sb.append(evt.asCharacters().getData)
      else if (evt.isEndElement) done = true
    }
    sb.toString()
  }

  private def decodeEnumeratedElement(
    reader: javax.xml.stream.XMLEventReader,
    start: StartElement,
    schema: Asn1Enumerated
  ): Any = {
    var name = ""
    var done = false
    while (reader.hasNext && !done) {
      val evt = reader.nextEvent()
      if (evt.isStartElement) {
        name = evt.asStartElement().getName.getLocalPart
        consumeUntilEndElement(reader, 1)
      } else if (evt.isEndElement) {
        done = true
      }
    }
    if (enumeratedAsInt) {
      schema.values.find(_.name == name).flatMap(_.value).getOrElse(0L)
    } else {
      UTF8String.fromString(name)
    }
  }

  private def decodeBitStringElement(
    reader: javax.xml.stream.XMLEventReader,
    start: StartElement,
    schema: Asn1BitString
  ): Any = {
    val text  = decodeText(reader, start).trim
    val bytes = hexStringToBytes(text)
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

  // -------------------------------------------------------------------------
  // SEQUENCE / SET
  // -------------------------------------------------------------------------

  private def decodeSequenceElement(
    reader: javax.xml.stream.XMLEventReader,
    start: StartElement,
    schema: Asn1Sequence
  ): InternalRow = {
    // Build a map: field name → Any value
    val fieldMap = scala.collection.mutable.Map.empty[String, Any]
    var done = false
    while (reader.hasNext && !done) {
      val evt = reader.nextEvent()
      if (evt.isStartElement) {
        val s    = evt.asStartElement()
        val name = s.getName.getLocalPart
        schema.components.find(_.name == name) match {
          case Some(comp) =>
            val v = decodeElement(reader, s, resolveRefs(comp.asn1Type))
            fieldMap(name) = v
          case None =>
            consumeToEnd(reader, s) // unknown field: skip
        }
      } else if (evt.isEndElement) {
        done = true
      }
    }
    val values = schema.components.map { comp =>
      fieldMap.getOrElse(comp.name, null)
    }
    new GenericInternalRow(values.toArray[Any])
  }

  // -------------------------------------------------------------------------
  // SEQUENCE OF / SET OF
  // -------------------------------------------------------------------------

  private def decodeSequenceOfElement(
    reader: javax.xml.stream.XMLEventReader,
    start: StartElement,
    schema: Asn1SequenceOf
  ): GenericArrayData = {
    val elems = scala.collection.mutable.ArrayBuffer.empty[Any]
    var done  = false
    while (reader.hasNext && !done) {
      val evt = reader.nextEvent()
      if (evt.isStartElement) {
        val v = decodeElement(reader, evt.asStartElement(), resolveRefs(schema.elementType))
        elems += v
      } else if (evt.isEndElement) {
        done = true
      }
    }
    new GenericArrayData(elems.toArray[Any])
  }

  // -------------------------------------------------------------------------
  // CHOICE
  // -------------------------------------------------------------------------

  private def decodeChoiceElement(
    reader: javax.xml.stream.XMLEventReader,
    start: StartElement,
    schema: Asn1Choice
  ): InternalRow = {
    // XER CHOICE: exactly one child element whose name identifies the alternative.
    val fields  = new Array[Any](1 + schema.alternatives.size)
    var done    = false
    while (reader.hasNext && !done) {
      val evt = reader.nextEvent()
      if (evt.isStartElement) {
        val s      = evt.asStartElement()
        val altName = s.getName.getLocalPart
        val altIdx  = schema.alternatives.indexWhere(_.name == altName)
        if (altIdx >= 0) {
          val alt       = schema.alternatives(altIdx)
          fields(0)     = UTF8String.fromString(alt.name)
          fields(altIdx + 1) = decodeElement(reader, s, resolveRefs(alt.asn1Type))
        } else {
          consumeToEnd(reader, s)
        }
      } else if (evt.isEndElement) {
        done = true
      }
    }
    if (fields(0) == null) fields(0) = UTF8String.fromString("unknown")
    new GenericInternalRow(fields)
  }

  // -------------------------------------------------------------------------
  // StAX helpers
  // -------------------------------------------------------------------------

  /** Consume events until the matching EndElement for `start` is reached. */
  private def consumeToEnd(reader: javax.xml.stream.XMLEventReader, start: StartElement): Unit =
    consumeUntilEndElement(reader, 1)

  private def consumeUntilEndElement(reader: javax.xml.stream.XMLEventReader, depth: Int): Unit = {
    var d = depth
    while (reader.hasNext && d > 0) {
      val evt = reader.nextEvent()
      if (evt.isStartElement) d += 1
      else if (evt.isEndElement) d -= 1
    }
  }

  // -------------------------------------------------------------------------
  // Reference resolution
  // -------------------------------------------------------------------------

  private def resolveRefs(t: Asn1Type): Asn1Type = t match {
    case ref: Asn1TypeReference =>
      val resolved = registry.resolveRef(ref, moduleName)
      if (resolved eq ref) ref else resolveRefs(resolved)
    case _ => t
  }

  // -------------------------------------------------------------------------
  // Hex / byte helpers
  // -------------------------------------------------------------------------

  private def hexStringToBytes(hex: String): Array[Byte] = {
    val clean = hex.replaceAll("\\s+", "")
    if (clean.isEmpty) return Array.empty
    require(clean.length % 2 == 0, s"Odd hex string in XER: ${clean.length} chars")
    val bytes = new Array[Byte](clean.length / 2)
    var i = 0
    while (i < bytes.length) {
      bytes(i) = java.lang.Integer.parseInt(clean.substring(i * 2, i * 2 + 2), 16).toByte
      i += 1
    }
    bytes
  }
}

class XerDecodeException(msg: String, cause: Throwable = null)
  extends RuntimeException(msg, cause)
