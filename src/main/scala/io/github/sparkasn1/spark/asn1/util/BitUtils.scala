package io.github.sparkasn1.spark.asn1.util

/** Bit-level utilities shared by PER encoder/decoder (Phase 2). */
object BitUtils {

  /** Ceiling of log2(n), i.e. the minimum number of bits to represent values 0..n-1. */
  def ceilLog2(n: Long): Int = {
    if (n <= 1) 0
    else (java.lang.Long.SIZE - java.lang.Long.numberOfLeadingZeros(n - 1)).toInt
  }

  /** Minimum bits needed to represent the range [min, max] inclusive. */
  def bitsForRange(min: Long, max: Long): Int = ceilLog2(max - min + 1)

  /** Extract bit at position `bitPos` (0 = MSB of first byte) from a byte array. */
  def getBit(bytes: Array[Byte], bitPos: Int): Int = {
    val byteIdx = bitPos / 8
    val bitIdx  = 7 - (bitPos % 8)
    if (byteIdx >= bytes.length) 0
    else (bytes(byteIdx) >> bitIdx) & 1
  }

  /** Read `n` bits starting at `bitOffset` (MSB first) from a byte array, return as Long. */
  def readBits(bytes: Array[Byte], bitOffset: Int, n: Int): Long = {
    var result = 0L
    var i      = 0
    while (i < n) {
      result = (result << 1) | getBit(bytes, bitOffset + i)
      i += 1
    }
    result
  }
}
