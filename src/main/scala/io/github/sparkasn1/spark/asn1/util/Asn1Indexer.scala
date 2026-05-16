package io.github.sparkasn1.spark.asn1.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileStatus, FileSystem, Path}
import org.apache.spark.sql.SparkSession

import java.io.{DataInputStream, DataOutputStream, InputStream}
import java.nio.ByteBuffer

/**
 * Builds and reads a sidecar record-offset index for BER/DER files, enabling
 * Spark to split large files across multiple tasks.
 *
 * Index file format (`<original>.asn1idx`):
 *   - 8-byte header: 7-byte magic "ASN1IDX" + 1-byte version (0x01)
 *   - 8 bytes per record: big-endian Long byte offset of the record's first tag byte
 *
 * Only definite-length BER (and all DER) can be indexed.  Indefinite-length
 * constructions stop the scan early and produce a partial index.
 *
 * == Reading efficiency ==
 *
 * `readIndexSlice` uses HDFS positioned reads (pread) to binary-search the index
 * file for the split boundaries, then reads only the matching slice sequentially.
 * For a 100 M-record index (~800 MB), this costs ~27 pread round-trips per task
 * plus a small sequential read — the full index is never loaded into memory.
 */
object Asn1Indexer {

  val INDEX_SUFFIX  = ".asn1idx"
  private val MAGIC = Array[Byte]('A','S','N','1','I','D','X', 0x01.toByte)

  // -------------------------------------------------------------------------
  // Single-file index construction
  // -------------------------------------------------------------------------

  /**
   * Scan `filePath` sequentially and write a sidecar index next to it.
   * Returns `(indexPath, recordCount)`.
   *
   * Safe to call on a file that already has an index — the existing index is
   * overwritten.  Use `buildIndexes` to skip already-indexed files.
   */
  def buildIndex(filePath: Path, conf: Configuration): (Path, Long) = {
    val fs        = filePath.getFileSystem(conf)
    val indexPath = new Path(filePath.toString + INDEX_SUFFIX)
    val stream    = fs.open(filePath)
    val idxOut    = fs.create(indexPath, /* overwrite */ true)
    var count     = 0L
    try {
      val out = new DataOutputStream(idxOut)
      out.write(MAGIC)
      var position = 0L
      var go       = true
      while (go) {
        val start     = position
        val recordLen = scanRecord(stream)
        if (recordLen <= 0) {
          go = false
        } else {
          out.writeLong(start)
          position += recordLen
          count    += 1
        }
      }
      out.flush()
    } finally {
      try stream.close()  catch { case _: Exception => () }
      try idxOut.close()  catch { case _: Exception => () }
    }
    (indexPath, count)
  }

  // -------------------------------------------------------------------------
  // Bulk indexing — parallel across files using Spark tasks
  // -------------------------------------------------------------------------

  /**
   * Index all BER/DER files matched by `glob` in parallel, one Spark task per
   * file.  Tasks run on executors co-located with the HDFS data blocks, so the
   * sequential scan is local I/O on each node.
   *
   * @param spark     active SparkSession
   * @param glob      HDFS glob or directory path, e.g. `"/data/cdrs/file.ber"` or a glob
   * @param overwrite if false (default), files that already have a `.asn1idx`
   *                  sidecar are skipped
   * @return          a DataFrame with one row per file:
   *                  (path: String, records: Long, skipped: Boolean)
   */
  def buildIndexes(
    spark:     SparkSession,
    glob:      String,
    overwrite: Boolean = false
  ): org.apache.spark.sql.DataFrame = {

    import spark.implicits._

    val conf  = spark.sparkContext.hadoopConfiguration
    val files = resolveFiles(glob, conf)

    val toProcess = files.filter { s =>
      val p = s.getPath
      overwrite || !p.getFileSystem(conf).exists(new Path(p.toString + INDEX_SUFFIX))
    }

    val allPaths     = files.map(_.getPath.toString)
    val pendingPaths = toProcess.map(_.getPath.toString)

    // Skipped files (already indexed, overwrite=false)
    val skippedRdd = spark.sparkContext
      .parallelize(allPaths.diff(pendingPaths), 1)
      .map(p => (p, 0L, true))

    // Files to index — one partition per file for data locality
    val broadcastConf = spark.sparkContext.broadcast(new SerializableHadoopConf(conf))
    val indexedRdd = spark.sparkContext
      .parallelize(pendingPaths, math.max(1, pendingPaths.length))
      .map { pathStr =>
        val c             = broadcastConf.value.get
        val (_, recCount) = buildIndex(new Path(pathStr), c)
        (pathStr, recCount, false)
      }

    (skippedRdd ++ indexedRdd).toDF("path", "records", "skipped")
  }

  // -------------------------------------------------------------------------
  // Index reading — range-read via binary search, never loads the full file
  // -------------------------------------------------------------------------

  /**
   * Return the byte offsets of all records whose start falls within
   * [fromByte, toExclByte).
   *
   * Uses two binary searches (O(log N) positioned reads) on the index file,
   * then one sequential read of only the matching slice.
   */
  def readIndexSlice(
    indexPath:  Path,
    conf:       Configuration,
    fromByte:   Long,
    toExclByte: Long
  ): Array[Long] = {
    val fs         = indexPath.getFileSystem(conf)
    val numEntries = ((fs.getFileStatus(indexPath).getLen - MAGIC.length) / 8).toInt
    if (numEntries == 0) return Array.empty[Long]

    val stream = fs.open(indexPath)
    try {
      val buf      = new Array[Byte](8)
      val startIdx = lowerBound(stream, numEntries, fromByte,   buf)
      val endIdx   = lowerBound(stream, numEntries, toExclByte, buf)
      if (endIdx <= startIdx) return Array.empty[Long]

      val count   = endIdx - startIdx
      val offsets = new Array[Long](count)
      stream.seek(MAGIC.length + startIdx.toLong * 8)
      val din = new DataInputStream(stream)
      var i = 0
      while (i < count) { offsets(i) = din.readLong(); i += 1 }
      offsets
    } finally {
      try stream.close() catch { case _: Exception => () }
    }
  }

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  /** Resolve a glob or directory path to a flat list of data files. */
  private def resolveFiles(glob: String, conf: Configuration): Seq[FileStatus] = {
    val path = new Path(glob)
    val fs   = path.getFileSystem(conf)
    val raw  = Option(fs.globStatus(path)).getOrElse(Array.empty[FileStatus])
    raw.flatMap { s =>
      if (s.isDirectory) fs.listStatus(s.getPath)   // one level deep
      else               Array(s)
    }.filter { s =>
      !s.isDirectory && !s.getPath.getName.endsWith(INDEX_SUFFIX)
    }.toSeq
  }

  /** Find the first entry index whose value is >= target (lower_bound). */
  private def lowerBound(
    stream: FSDataInputStream,
    n:      Int,
    target: Long,
    buf:    Array[Byte]
  ): Int = {
    var lo = 0
    var hi = n
    while (lo < hi) {
      val mid = (lo + hi) >>> 1
      stream.readFully(MAGIC.length + mid.toLong * 8, buf)
      if (ByteBuffer.wrap(buf).getLong() < target) lo = mid + 1 else hi = mid
    }
    lo
  }

  // -------------------------------------------------------------------------
  // BER/DER TLV scanner
  // Returns total record length (tag + length + value bytes) or -1 on stop.
  // -------------------------------------------------------------------------

  private def scanRecord(in: InputStream): Long = {
    val t0 = in.read()
    if (t0 < 0) return -1
    var tagLen = 1L
    if ((t0 & 0x1F) == 0x1F) {
      var b = 0
      do {
        b = in.read()
        if (b < 0) return -1
        tagLen += 1
      } while ((b & 0x80) != 0)
    }

    val l0 = in.read()
    if (l0 < 0) return -1
    var lenLen   = 1L
    var valueLen = 0L
    if (l0 <= 0x7F) {
      valueLen = l0
    } else if (l0 == 0x80) {
      return -1                          // indefinite length — stop
    } else {
      val numBytes = l0 & 0x7F
      lenLen += numBytes
      var i = 0
      while (i < numBytes) {
        val b = in.read()
        if (b < 0) return -1
        valueLen = (valueLen << 8) | b
        i += 1
      }
    }

    skipExactly(in, valueLen)
    tagLen + lenLen + valueLen
  }

  private def skipExactly(in: InputStream, n: Long): Unit = {
    var rem = n
    while (rem > 0) {
      val skipped = in.skip(rem)
      if (skipped <= 0) return
      rem -= skipped
    }
  }
}

