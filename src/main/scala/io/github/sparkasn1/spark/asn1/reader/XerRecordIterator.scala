package io.github.sparkasn1.spark.asn1.reader

import io.github.sparkasn1.spark.asn1.codec.xer.XerDecoder
import io.github.sparkasn1.spark.asn1.model.Asn1Type
import org.apache.spark.sql.catalyst.InternalRow

import java.io.{ByteArrayInputStream, InputStream, SequenceInputStream, StringReader}
import javax.xml.stream.{XMLInputFactory, XMLStreamConstants, XMLStreamReader}

/**
 * Splits a stream of concatenated XER records into individual elements and
 * decodes each one.
 *
 * Java's StAX parser treats the input as a single-root XML document and stops
 * after the first root element.  To work around this, we wrap the raw stream in
 * a synthetic `<_xer_root_>…</_xer_root_>` envelope so the parser sees a valid
 * single document, then extract records at depth 2.
 *
 * Wrapper detection: if the depth-2 element cannot be decoded as a record
 * (e.g. `<Records><id>…</id></Records>`), its children are extracted and
 * decoded individually.  Wrapper-extracted records beyond the first are
 * buffered in `pendingRows`.
 */
class XerRecordIterator(
  stream: InputStream,
  decoder: XerDecoder,
  rootSchema: Asn1Type,
  requiredNames: Set[String] = Set.empty
) extends Iterator[InternalRow] with AutoCloseable {

  private val WRAP_OPEN  = "<_xer_root_>".getBytes("UTF-8")
  private val WRAP_CLOSE = "</_xer_root_>".getBytes("UTF-8")

  private val factory: XMLInputFactory = {
    val f = XMLInputFactory.newInstance()
    f.setProperty(XMLInputFactory.IS_COALESCING, true)
    f.setProperty(XMLInputFactory.IS_NAMESPACE_AWARE, false)
    f.setProperty(XMLInputFactory.IS_REPLACING_ENTITY_REFERENCES, false)
    f.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, false)
    f
  }

  // Wrap raw stream so StAX sees a valid single-root document.
  private val wrappedStream: InputStream =
    new SequenceInputStream(
      new SequenceInputStream(
        new ByteArrayInputStream(WRAP_OPEN),
        stream),
      new ByteArrayInputStream(WRAP_CLOSE))

  private val streamReader: XMLStreamReader =
    factory.createXMLStreamReader(wrappedStream, "UTF-8")

  private var depth = 0

  // Records extracted from a wrapper element are buffered here.
  private val pendingRows = scala.collection.mutable.Queue[InternalRow]()

  private var _next: Option[InternalRow] = advance()
  private var closed = false

  override def hasNext: Boolean = _next.isDefined

  override def next(): InternalRow = {
    val row = _next.getOrElse(throw new NoSuchElementException("iterator exhausted"))
    _next = if (pendingRows.nonEmpty) Some(pendingRows.dequeue()) else advance()
    if (!hasNext) close()
    row
  }

  override def close(): Unit = if (!closed) {
    closed = true
    try streamReader.close() catch { case _: Exception => () }
    try stream.close()       catch { case _: Exception => () }
  }

  // -------------------------------------------------------------------------
  // Advance: find the next record element and decode it
  // -------------------------------------------------------------------------

  private def advance(): Option[InternalRow] = {
    try {
      while (streamReader.hasNext) {
        streamReader.next() match {
          case XMLStreamConstants.START_ELEMENT =>
            depth += 1
            if (depth == 2) {
              val (xml, hasDirectText) = captureElement(streamReader)
              depth -= 1
              if (!hasDirectText) {
                // No direct text — could be a SEQUENCE record or a wrapper element.
                // Try decoding first; if it fails, treat as a wrapper and extract children.
                val maybeRow = tryDecode(xml)
                if (maybeRow.isDefined) {
                  return maybeRow
                } else {
                  val childXmls = extractChildXmls(xml)
                  val decoded   = childXmls.flatMap(tryDecode)
                  if (decoded.nonEmpty) {
                    pendingRows ++= decoded.tail
                    return Some(decoded.head)
                  }
                }
              } else {
                return tryDecode(xml)
              }
            }

          case XMLStreamConstants.END_ELEMENT =>
            depth -= 1
            if (depth < 1) return None  // end of the synthetic root

          case _ =>
        }
      }
      None
    } catch { case _: Exception => None }
  }

  // -------------------------------------------------------------------------
  // Extract direct-child XML strings from a wrapper element string
  // -------------------------------------------------------------------------

  private def extractChildXmls(wrapperXml: String): List[String] = {
    val reader  = factory.createXMLStreamReader(new StringReader(wrapperXml))
    val results = scala.collection.mutable.ListBuffer[String]()
    var d = 0
    try {
      while (reader.hasNext) {
        reader.next() match {
          case XMLStreamConstants.START_ELEMENT =>
            d += 1
            if (d == 2) {
              val (xml, _) = captureElement(reader)
              d -= 1
              results += xml
            }
          case XMLStreamConstants.END_ELEMENT =>
            d -= 1
          case _ =>
        }
      }
    } finally {
      try reader.close() catch { case _: Exception => () }
    }
    results.toList
  }

  // -------------------------------------------------------------------------
  // Capture the current element (opening tag already consumed) as XML string.
  // Returns (xmlString, hasDirectText).
  // -------------------------------------------------------------------------

  private def captureElement(reader: XMLStreamReader): (String, Boolean) = {
    val sb  = new StringBuilder
    val tag = reader.getLocalName
    sb.append('<').append(tag)
    (0 until reader.getAttributeCount).foreach { i =>
      sb.append(' ')
        .append(reader.getAttributeLocalName(i))
        .append("=\"")
        .append(escapeXml(reader.getAttributeValue(i)))
        .append('"')
    }
    sb.append('>')

    var innerDepth    = 1
    var hasDirectText = false

    while (reader.hasNext && innerDepth > 0) {
      reader.next() match {
        case XMLStreamConstants.START_ELEMENT =>
          innerDepth += 1
          sb.append('<').append(reader.getLocalName)
          (0 until reader.getAttributeCount).foreach { i =>
            sb.append(' ')
              .append(reader.getAttributeLocalName(i))
              .append("=\"")
              .append(escapeXml(reader.getAttributeValue(i)))
              .append('"')
          }
          sb.append('>')

        case XMLStreamConstants.END_ELEMENT =>
          innerDepth -= 1
          if (innerDepth >= 0)
            sb.append("</").append(reader.getLocalName).append('>')

        case XMLStreamConstants.CHARACTERS | XMLStreamConstants.CDATA =>
          val text = reader.getText
          if (innerDepth == 1 && text.trim.nonEmpty) hasDirectText = true
          sb.append(escapeXml(text))

        case _ =>
      }
    }
    (sb.toString(), hasDirectText)
  }

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  private def tryDecode(xml: String): Option[InternalRow] =
    try Some(decoder.decodeXml(xml, rootSchema, requiredNames))
    catch { case _: Exception => None }

  private def escapeXml(s: String): String =
    s.replace("&", "&amp;")
     .replace("<", "&lt;")
     .replace(">", "&gt;")
     .replace("\"", "&quot;")
}
