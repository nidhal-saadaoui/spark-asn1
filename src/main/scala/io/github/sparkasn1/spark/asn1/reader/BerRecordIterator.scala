package io.github.sparkasn1.spark.asn1.reader

import io.github.sparkasn1.spark.asn1.codec.BerDerDecoder
import io.github.sparkasn1.spark.asn1.model.Asn1Type
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

import java.io.InputStream

/**
 * Iterator that reads successive ASN.1 BER/DER records from an InputStream,
 * decoding each one against the provided schema.
 *
 * The stream is closed when the iterator is exhausted or when close() is called.
 *
 * @param maxRecords stop after decoding this many records (used when reading
 *                   a split slice identified via a sidecar index).
 * @param sourceName display name used in log messages (typically the file path).
 */
class BerRecordIterator(
  stream:         InputStream,
  decoder:        BerDerDecoder,
  rootSchema:     Asn1Type,
  maxRecords:     Long               = Long.MaxValue,
  requiredSchema: Option[StructType] = None,
  sourceName:     String             = ""
) extends Iterator[InternalRow] with AutoCloseable {

  private val log = LoggerFactory.getLogger(classOf[BerRecordIterator])

  private val parser: InputStream = decoder.openParser(stream)
  private var decoded  = 0L
  private var nextRow: Option[InternalRow] = prefetch()

  if (nextRow.isEmpty && maxRecords > 0) {
    val src = if (sourceName.nonEmpty) s" ($sourceName)" else ""
    log.warn(
      s"BER/DER decoder produced 0 records$src. " +
      "Common causes: wrong asn1.type name, wrong asn1.encoding, " +
      "or file is not valid BER/DER. Use Asn1Inspector.peek() to diagnose.")
  }

  private def prefetch(): Option[InternalRow] = {
    if (decoded >= maxRecords) return None
    try decoder.decodeNext(parser, rootSchema, requiredSchema)
    catch { case _: Exception => None }
  }

  override def hasNext: Boolean = nextRow.isDefined

  override def next(): InternalRow = {
    val row = nextRow.getOrElse(throw new NoSuchElementException("iterator exhausted"))
    decoded += 1
    nextRow = prefetch()
    if (!hasNext) close()
    row
  }

  override def close(): Unit =
    try stream.close() catch { case _: Exception => () }
}
