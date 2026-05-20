package io.github.sparkasn1.spark.asn1.codec.per

import java.io.ByteArrayOutputStream

/**
 * Bit-level output buffer for PER encoding.
 *
 * Bits are written MSB-first within each byte.
 * Call `toByteArray` once at the end to get the final bytes (partial trailing byte is zero-padded).
 */
class PerBitOutput(val aligned: Boolean) {

  private val out          = new ByteArrayOutputStream(64)
  private var currentByte  = 0
  private var bitsInCurrent = 0  // bits written into currentByte so far (0..7)

  // -------------------------------------------------------------------------
  // Write primitives
  // -------------------------------------------------------------------------

  def writeBit(bit: Int): Unit = {
    currentByte = (currentByte << 1) | (bit & 1)
    bitsInCurrent += 1
    if (bitsInCurrent == 8) flushByte()
  }

  def writeBits(value: Long, n: Int): Unit = {
    var i = n - 1
    while (i >= 0) { writeBit(((value >> i) & 1).toInt); i -= 1 }
  }

  /** Align to next byte boundary (no-op if already aligned). */
  def align(): Unit =
    if (bitsInCurrent > 0) {
      currentByte <<= (8 - bitsInCurrent)
      flushByte()
    }

  /** Write a full byte. In aligned mode, aligns first. */
  def writeByte(b: Int): Unit = {
    if (aligned) align()
    writeBits(b & 0xff, 8)
  }

  /** Write all bytes. In aligned mode, aligns first. */
  def writeBytes(bs: Array[Byte]): Unit = {
    if (aligned) align()
    bs.foreach(b => writeBits(b & 0xff, 8))
  }

  // -------------------------------------------------------------------------
  // Finalise
  // -------------------------------------------------------------------------

  def toByteArray: Array[Byte] = {
    if (bitsInCurrent > 0) {
      out.write(currentByte << (8 - bitsInCurrent))
    }
    out.toByteArray
  }

  // -------------------------------------------------------------------------
  // Internal
  // -------------------------------------------------------------------------

  private def flushByte(): Unit = {
    out.write(currentByte & 0xff)
    currentByte    = 0
    bitsInCurrent = 0
  }
}
