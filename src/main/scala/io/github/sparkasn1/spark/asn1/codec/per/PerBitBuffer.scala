package io.github.sparkasn1.spark.asn1.codec.per

import java.io.EOFException

/**
 * Bit-level reader over a byte array for PER decoding.
 *
 * All reads are MSB-first. `align()` advances the cursor to the next byte boundary;
 * it is a no-op for unaligned PER.
 */
class PerBitBuffer(private val bytes: Array[Byte]) {

  private var pos: Int = 0 // current bit position (0 = MSB of bytes(0))

  def bitsRemaining: Int = bytes.length * 8 - pos
  def isExhausted: Boolean = pos >= bytes.length * 8

  def readBit(): Int = {
    if (isExhausted) throw new EOFException("PerBitBuffer exhausted")
    val byteIdx = pos / 8
    val bitIdx  = 7 - (pos % 8)
    pos += 1
    (bytes(byteIdx) >> bitIdx) & 1
  }

  def readBits(n: Int): Long = {
    if (n == 0) return 0L
    if (n > 64) throw new IllegalArgumentException(s"Cannot read $n bits into Long")
    var result = 0L
    var i = 0
    while (i < n) { result = (result << 1) | readBit(); i += 1 }
    result
  }

  def readByte(): Int = readBits(8).toInt

  /** Advance to the next byte boundary (aligned PER). No-op if already aligned. */
  def align(): Unit = {
    val rem = pos % 8
    if (rem != 0) pos += (8 - rem)
  }

  /** Read `n` whole bytes (after optional alignment) into a new array. */
  def readByteArray(n: Int): Array[Byte] = {
    val arr = new Array[Byte](n)
    var i = 0
    while (i < n) { arr(i) = readBits(8).toByte; i += 1 }
    arr
  }

  /** Current bit position (for debugging). */
  def bitPosition: Int = pos
}

object PerBitBuffer {
  def apply(bytes: Array[Byte]): PerBitBuffer = new PerBitBuffer(bytes)
}
