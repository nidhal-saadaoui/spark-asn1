package io.github.sparkasn1.spark.asn1.util

/**
 * BER/DER encoding of ASN.1 REAL (X.690 §8.5) without BouncyCastle support.
 *
 * Only the binary encoding (base-2) is produced. Special values +∞/-∞ use
 * their standardised single-byte representations. Zero maps to empty content.
 */
object BerRealUtil {

  /** Encode a Double as BER REAL *content* bytes (no tag or length wrapper). */
  def encodeContent(d: Double): Array[Byte] = {
    if (d.isNaN)            return Array(0x42.toByte)
    if (d == Double.PositiveInfinity) return Array(0x40.toByte)
    if (d == Double.NegativeInfinity) return Array(0x41.toByte)
    if (d == 0.0)           return Array.empty[Byte]

    val bits       = java.lang.Double.doubleToLongBits(d)
    val sign       = if (bits < 0) 1 else 0
    val biasedExp  = ((bits >>> 52) & 0x7ffL).toInt
    val mantissa   = bits & 0x000fffffffffffffL

    // N * 2^E = |d|
    val (rawN, rawE) =
      if (biasedExp == 0) (mantissa, -1022 - 52)       // subnormal
      else ((1L << 52) | mantissa, biasedExp - 1023 - 52)

    // Strip trailing zero bits from N, absorb into exponent
    var n = rawN
    var e = rawE
    while ((n & 1L) == 0L && n != 0L) { n >>>= 1; e += 1 }

    val expBytes = signedMinimalBytes(e)
    val nBytes   = unsignedMinimalBytes(n)

    val expLenField: Int = expBytes.length match {
      case 1 => 0x00
      case 2 => 0x01
      case 3 => 0x02
      case _ => 0x03  // long-form exponent (≥4 bytes; not needed for IEEE 754)
    }
    val infoByte = (0x80 | (sign << 6) | expLenField).toByte

    val result = new Array[Byte](1 + expBytes.length + nBytes.length)
    result(0) = infoByte
    System.arraycopy(expBytes, 0, result, 1, expBytes.length)
    System.arraycopy(nBytes, 0, result, 1 + expBytes.length, nBytes.length)
    result
  }

  /** Decode BER REAL *content* bytes into a Double. */
  def decodeContent(bytes: Array[Byte]): Double = {
    if (bytes.isEmpty)          return 0.0
    val first = bytes(0) & 0xff
    if (first == 0x40)          return Double.PositiveInfinity
    if (first == 0x41)          return Double.NegativeInfinity
    if (first == 0x42)          return Double.NaN
    if ((first & 0x80) == 0)   return decodeIso6093(bytes)  // decimal form (rare)

    val sign          = (first >> 6) & 1
    val scalingFactor = (first >> 2) & 3   // scaling of mantissa
    val expLenField   = first & 3

    var offset = 1
    val expLen = if (expLenField == 3) {
      val l = bytes(offset) & 0xff; offset += 1; l
    } else expLenField + 1

    var exp = if ((bytes(offset) & 0x80) != 0) -1L else 0L
    (0 until expLen).foreach { i => exp = (exp << 8) | (bytes(offset + i) & 0xff) }
    offset += expLen

    var n = 0L
    while (offset < bytes.length) { n = (n << 8) | (bytes(offset) & 0xff); offset += 1 }

    val value = java.lang.Math.scalb(n.toDouble, (exp + scalingFactor).toInt)
    if (sign == 1) -value else value
  }

  /** Build a complete BER TLV for a REAL value (tag 0x09). */
  def buildTlv(content: Array[Byte]): Array[Byte] = {
    val len = content.length
    val lenBytes = if (len <= 127) Array(len.toByte)
                   else Array((0x81).toByte, len.toByte)
    val result = new Array[Byte](1 + lenBytes.length + len)
    result(0) = 0x09.toByte
    System.arraycopy(lenBytes, 0, result, 1, lenBytes.length)
    System.arraycopy(content, 0, result, 1 + lenBytes.length, len)
    result
  }

  private def decodeIso6093(bytes: Array[Byte]): Double =
    new String(bytes, "US-ASCII").trim.toDouble

  private def signedMinimalBytes(v: Long): Array[Byte] = {
    if (v == 0L) return Array(0x00.toByte)
    val buf = new Array[Byte](8)
    var x = v; var i = 7
    while (i >= 0) { buf(i) = (x & 0xff).toByte; x >>= 8; i -= 1 }
    var start = 0
    while (start < 7 &&
      ((buf(start) == 0x00.toByte && (buf(start + 1) & 0x80) == 0) ||
       (buf(start) == 0xff.toByte && (buf(start + 1) & 0x80) != 0)))
      start += 1
    buf.slice(start, 8)
  }

  private def unsignedMinimalBytes(v: Long): Array[Byte] = {
    if (v == 0L) return Array(0x00.toByte)
    val buf = new Array[Byte](8)
    var x = v; var i = 7
    while (i >= 0) { buf(i) = (x & 0xff).toByte; x >>= 8; i -= 1 }
    var start = 0
    while (start < 7 && buf(start) == 0x00.toByte) start += 1
    buf.slice(start, 8)
  }
}
