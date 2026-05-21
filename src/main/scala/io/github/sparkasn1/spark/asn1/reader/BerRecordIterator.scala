package io.github.sparkasn1.spark.asn1.reader

import io.github.sparkasn1.spark.asn1.codec.BerDerDecoder
import io.github.sparkasn1.spark.asn1.model.Asn1Type
import org.apache.spark.sql.catalyst.InternalRow

import java.io.InputStream

/**
 * Iterator that reads successive ASN.1 BER/DER records from an InputStream,
 * decoding each one against the provided schema.
 *
 * The stream is closed when the iterator is exhausted or when close() is called.
 */
/**
 * @param maxRecords stop after decoding this many records (used when reading
 *                   a split slice identified via a sidecar index).
 */
class BerRecordIterator(
  stream:         InputStream,
  decoder:        BerDerDecoder,
  rootSchema:     Asn1Type,
  maxRecords:     Long                = Long.MaxValue,
  requiredFields: Option[Seq[String]] = None
) extends Iterator[InternalRow] with AutoCloseable {

  private val parser: InputStream = decoder.openParser(stream)
  private var decoded  = 0L
  private var nextRow: Option[InternalRow] = prefetch()

  private def prefetch(): Option[InternalRow] = {
    if (decoded >= maxRecords) return None
    try decoder.decodeNext(parser, rootSchema, requiredFields)
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
