package io.github.sparkasn1.spark.asn1.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.bouncycastle.asn1._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.{File, FileOutputStream}
import java.nio.file.Files

class Asn1IndexerSpec extends AnyFlatSpec with Matchers {

  private val conf = new Configuration()
  conf.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)
  conf.set("fs.file.impl.disable.cache", "true")

  // Build a multi-record BER file from DER-encoded sequences.
  // Returns (tempFile, recordOffsets).
  private def makeBerFile(records: Seq[Array[Byte]]): (File, Seq[Long]) = {
    val f   = Files.createTempFile("asn1-idx-test", ".ber").toFile
    val fos = new FileOutputStream(f)
    val offsets = scala.collection.mutable.ArrayBuffer[Long]()
    var pos = 0L
    try records.foreach { r =>
      offsets += pos
      fos.write(r)
      pos += r.length
    } finally fos.close()
    (f, offsets.toSeq)
  }

  private def der(o: ASN1Primitive): Array[Byte] = o.getEncoded("DER")

  private val recs: Seq[Array[Byte]] = Seq(
    der(new DERSequence(Array[ASN1Encodable](new ASN1Integer(1), new DERUTF8String("Alice")))),
    der(new DERSequence(Array[ASN1Encodable](new ASN1Integer(2), new DERUTF8String("Bob")))),
    der(new DERSequence(Array[ASN1Encodable](new ASN1Integer(3), new DERUTF8String("Carol")))),
    der(new DERSequence(Array[ASN1Encodable](new ASN1Integer(4), new DERUTF8String("Dave")))),
    der(new DERSequence(Array[ASN1Encodable](new ASN1Integer(5), new DERUTF8String("Eve"))))
  )

  "Asn1Indexer.buildIndex" should "produce one offset entry per record" in {
    val (f, _) = makeBerFile(recs)
    val (idxPath, count) = Asn1Indexer.buildIndex(new Path(f.toURI), conf)
    try {
      count shouldBe 5L
      val fs = FileSystem.get(idxPath.toUri, conf)
      fs.exists(idxPath) shouldBe true
      // index file: 8-byte magic + 5 × 8-byte entries = 48 bytes
      fs.getFileStatus(idxPath).getLen shouldBe (8 + 5 * 8).toLong
    } finally {
      f.delete()
      FileSystem.get(idxPath.toUri, conf).delete(idxPath, false)
    }
  }

  it should "record the correct byte offsets for each record" in {
    val (f, expectedOffsets) = makeBerFile(recs)
    val (idxPath, _)         = Asn1Indexer.buildIndex(new Path(f.toURI), conf)
    try {
      // Each record offset must be within the full file range
      val fileLen = f.length()
      val allSlice = Asn1Indexer.readIndexSlice(idxPath, conf, 0L, fileLen)
      allSlice.length shouldBe 5
      allSlice.toSeq shouldBe expectedOffsets
    } finally {
      f.delete()
      FileSystem.get(idxPath.toUri, conf).delete(idxPath, false)
    }
  }

  "Asn1Indexer.readIndexSlice" should "return only records whose start falls in [from, to)" in {
    val (f, offsets) = makeBerFile(recs)
    val (idxPath, _) = Asn1Indexer.buildIndex(new Path(f.toURI), conf)
    try {
      // Split at the midpoint of record 2 → should include records 0,1,2
      val mid = offsets(2) + 1L
      val lo  = Asn1Indexer.readIndexSlice(idxPath, conf, 0L,   mid)
      val hi  = Asn1Indexer.readIndexSlice(idxPath, conf, mid,  f.length())

      lo.length shouldBe 3
      hi.length shouldBe 2
      lo.toSeq shouldBe offsets.take(3)
      hi.toSeq shouldBe offsets.drop(3)
    } finally {
      f.delete()
      FileSystem.get(idxPath.toUri, conf).delete(idxPath, false)
    }
  }

  it should "return empty when the byte range contains no record starts" in {
    val (f, offsets) = makeBerFile(recs)
    val (idxPath, _) = Asn1Indexer.buildIndex(new Path(f.toURI), conf)
    try {
      // Range entirely inside the first record's body (no record STARTS there)
      val inside = Asn1Indexer.readIndexSlice(idxPath, conf, 1L, offsets(1))
      inside shouldBe empty
    } finally {
      f.delete()
      FileSystem.get(idxPath.toUri, conf).delete(idxPath, false)
    }
  }

  it should "handle a single-record file" in {
    val (f, offsets) = makeBerFile(recs.take(1))
    val (idxPath, count) = Asn1Indexer.buildIndex(new Path(f.toURI), conf)
    try {
      count shouldBe 1L
      val slice = Asn1Indexer.readIndexSlice(idxPath, conf, 0L, f.length())
      slice.toSeq shouldBe offsets
    } finally {
      f.delete()
      FileSystem.get(idxPath.toUri, conf).delete(idxPath, false)
    }
  }

  "Asn1Indexer" should "work through Spark's isSplitable path via DataSource" in {
    import org.apache.spark.sql.SparkSession
    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("indexer-splittable-test")
      .config("spark.ui.enabled", "false")
      .config("spark.hadoop.fs.file.impl.disable.cache", "true")
      .getOrCreate()
    try {
      val schemaDir = new File(getClass.getResource("/schemas").getPath)
      val schemaPath = new File(schemaDir, "simple.asn1").getAbsolutePath

      // Build a 5-record BER file in /tmp
      val berFile = Files.createTempFile("idx-spark", ".ber").toFile
      val fos = new FileOutputStream(berFile)
      try recs.foreach(fos.write) finally fos.close()

      // Build the index
      Asn1Indexer.buildIndex(new Path(berFile.toURI), conf)

      // Read through the data source — the index makes the file splittable
      val df = spark.read
        .format("asn1")
        .option("asn1.schema",   schemaPath)
        .option("asn1.type",     "SimpleRecord")
        .option("asn1.encoding", "ber")
        .load(berFile.getAbsolutePath)

      // SimpleRecord has id and name as first two fields
      df.count() shouldBe 5L
      val names = df.select("name").collect().map(_.getString(0)).toSet
      names shouldBe Set("Alice", "Bob", "Carol", "Dave", "Eve")

      // SimpleRecord schema: id INTEGER, name UTF8String, ...
      // Note: recs only have 2 fields but schema expects 6 — other fields will be absent
      // (this tests that missing fields are handled gracefully)

      berFile.delete()
      new File(berFile.getAbsolutePath + Asn1Indexer.INDEX_SUFFIX).delete()
    } finally spark.stop()
  }
}
