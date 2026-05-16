package io.github.sparkasn1.spark.asn1.reader

import io.github.sparkasn1.spark.asn1.codec.per.PerDecoder
import io.github.sparkasn1.spark.asn1.model.Asn1Type
import org.apache.spark.sql.catalyst.InternalRow

import java.io.{BufferedReader, InputStream, InputStreamReader}

/**
 * Iterator that reads successive PER-encoded records from an InputStream.
 *
 * Three framing strategies (controlled by `asn1.per.framing`):
 *  - `length-prefixed`: 4-byte big-endian length prefix per record
 *  - `fixed-length`: every record is exactly `recordBytes` bytes
 *  - `hex-lines`: each non-blank line is a hex-encoded record
 */
class PerRecordIterator(
  stream: InputStream,
  decoder: PerDecoder,
  rootSchema: Asn1Type,
  framing: String,
  recordBytes: Int = 0
) extends Iterator[InternalRow] with AutoCloseable {

  // For hex-lines framing we wrap the raw stream in a BufferedReader once.
  private val hexReader: Option[BufferedReader] = framing match {
    case "hex-lines" => Some(new BufferedReader(new InputStreamReader(stream, "UTF-8")))
    case _           => None
  }

  // fixedBuf must be initialized before _next (which calls readNext on construction).
  private val fixedBuf: Array[Byte] = {
    require(framing != "fixed-length" || recordBytes > 0,
      "asn1.per.record.bytes must be > 0 for fixed-length framing")
    new Array[Byte](if (framing == "fixed-length") recordBytes else 1)
  }

  private var _next: Option[InternalRow] = readNext()
  private var closed = false

  override def hasNext: Boolean = _next.isDefined

  override def next(): InternalRow = {
    val row = _next.getOrElse(throw new NoSuchElementException("iterator exhausted"))
    _next = readNext()
    if (!hasNext) close()
    row
  }

  override def close(): Unit = if (!closed) {
    closed = true
    try stream.close() catch { case _: Exception => () }
  }

  // -------------------------------------------------------------------------
  // Dispatch
  // -------------------------------------------------------------------------

  private def readNext(): Option[InternalRow] = framing match {
    case "fixed-length" => readFixedLength()
    case "hex-lines"    => readHexLine()
    case _              => readLengthPrefixed()
  }

  // -------------------------------------------------------------------------
  // Length-prefixed framing
  // -------------------------------------------------------------------------

  private def readLengthPrefixed(): Option[InternalRow] = {
    val lenBuf = new Array[Byte](4)
    if (readFully(stream, lenBuf, 4) < 4) return None
    val len = ((lenBuf(0) & 0xff) << 24) |
              ((lenBuf(1) & 0xff) << 16) |
              ((lenBuf(2) & 0xff) <<  8) |
               (lenBuf(3) & 0xff)
    if (len < 0) return None
    val recBuf = new Array[Byte](len)
    if (readFully(stream, recBuf, len) < len) return None
    tryDecode(recBuf)
  }

  // -------------------------------------------------------------------------
  // Fixed-length framing
  // -------------------------------------------------------------------------

  private def readFixedLength(): Option[InternalRow] = {
    if (readFully(stream, fixedBuf, recordBytes) < recordBytes) return None
    tryDecode(fixedBuf.clone())
  }

  // -------------------------------------------------------------------------
  // Hex-lines framing
  // -------------------------------------------------------------------------

  private def readHexLine(): Option[InternalRow] = {
    val reader = hexReader.get
    var line: String = null
    var result: Option[InternalRow] = None
    while (result.isEmpty && { line = reader.readLine(); line != null }) {
      val trimmed = line.trim.replaceAll("\\s+", "")
      if (trimmed.nonEmpty) {
        result = tryDecode(hexToBytes(trimmed))
      }
    }
    result
  }

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  private def tryDecode(bytes: Array[Byte]): Option[InternalRow] =
    try Some(decoder.decodeBytes(bytes, rootSchema))
    catch { case _: Exception => None }

  private def readFully(is: InputStream, buf: Array[Byte], n: Int): Int = {
    var total = 0
    while (total < n) {
      val read = is.read(buf, total, n - total)
      if (read < 0) return total
      total += read
    }
    total
  }

  private def hexToBytes(hex: String): Array[Byte] = {
    require(hex.length % 2 == 0, s"Odd-length hex string: ${hex.length}")
    val bytes = new Array[Byte](hex.length / 2)
    var i = 0
    while (i < bytes.length) {
      bytes(i) = java.lang.Integer.parseInt(hex.substring(i * 2, i * 2 + 2), 16).toByte
      i += 1
    }
    bytes
  }
}
