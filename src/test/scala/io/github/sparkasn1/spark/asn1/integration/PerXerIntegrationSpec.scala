package io.github.sparkasn1.spark.asn1.integration

import io.github.sparkasn1.spark.asn1.codec.per.{AlignedPerEncoder, UnalignedPerEncoder}
import io.github.sparkasn1.spark.asn1.codec.xer.XerEncoder
import io.github.sparkasn1.spark.asn1.util.SchemaCache
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

import java.io.{File, FileOutputStream}
import java.nio.file.{Files, Path}

/**
 * Integration tests for PER and XER encodings.
 *
 * Data is generated using PerEncoder / XerEncoder and then read back through
 * the Asn1DataSource to verify end-to-end round-trip correctness.
 *
 * We bypass Hadoop FileFormatWriter (fails on Java 25+) and write bytes
 * directly to disk using CatalystTypeConverters to convert Row → InternalRow.
 */
class PerXerIntegrationSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _
  private var tmpDir: Path        = _
  private val schemaDir = new File(getClass.getResource("/schemas").getPath)

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .master("local[2]")
      .appName("spark-asn1-per-xer-test")
      .config("spark.ui.enabled", "false")
      .config("spark.hadoop.fs.file.impl.disable.cache", "true")
      .config("spark.serializer.extraDebugInfo", "false")
      .getOrCreate()
    tmpDir = Files.createTempDirectory("spark-asn1-per-xer-test")
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
    deleteRecursively(tmpDir.toFile)
  }

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  private def deleteRecursively(f: File): Unit = {
    if (f.isDirectory) f.listFiles().foreach(deleteRecursively)
    f.delete()
  }

  private def normaliseRow(row: Row): Seq[Any] =
    (0 until row.size).map(i => row.get(i) match {
      case b: Array[Byte] => b.toSeq
      case v              => v
    })

  private def checkAnswer(df: org.apache.spark.sql.DataFrame, expected: Seq[Seq[Any]], sortCol: String): Unit = {
    val keyIdx = df.schema.fieldIndex(sortCol)
    val actual = df.collect().map(normaliseRow).toSeq
      .sortWith { (a, b) => a(keyIdx).asInstanceOf[Comparable[Any]].compareTo(b(keyIdx)) < 0 }
    val exp = expected.sortWith { (a, b) =>
      a(keyIdx).asInstanceOf[Comparable[Any]].compareTo(b(keyIdx)) < 0
    }
    actual.length shouldBe exp.length
    actual.zip(exp).zipWithIndex.foreach { case ((a, e), i) =>
      withClue(s"Row $i: actual=$a  expected=$e\n") { a shouldBe e }
    }
  }

  // Write length-prefixed PER records [4-byte-len][per-bytes] to a temp file.
  private def writePerFile(name: String, recordBytes: Seq[Array[Byte]]): File = {
    val f   = new File(tmpDir.toFile, name)
    val out = new FileOutputStream(f)
    try recordBytes.foreach { rec =>
      val len = rec.length
      out.write(Array[Byte](
        ((len >> 24) & 0xff).toByte, ((len >> 16) & 0xff).toByte,
        ((len >>  8) & 0xff).toByte, ( len        & 0xff).toByte))
      out.write(rec)
    }
    finally out.close()
    f
  }

  // Write XER records (already newline-terminated by XerEncoder) to a temp file.
  private def writeXerFile(name: String, recordBytes: Seq[Array[Byte]]): File = {
    val f   = new File(tmpDir.toFile, name)
    val out = new FileOutputStream(f)
    try recordBytes.foreach(out.write)
    finally out.close()
    f
  }

  // =========================================================================
  // APER — Aligned PER
  // =========================================================================

  "Asn1DataSource" should "read an APER file (length-prefixed) with SimpleRecord schema" in {
    val schemaPath = new File(schemaDir, "simple.asn1").getAbsolutePath
    val registry   = SchemaCache.getOrParse(Seq(schemaPath))
    val modName    = registry.modules.keys.head
    val rootType   = registry.resolve("SimpleRecord", modName).get
    val encoder    = new AlignedPerEncoder(registry, modName)

    // Build a minimal DataFrame so we can use CatalystTypeConverters.
    val df0 = spark.read
      .format("asn1")
      .option("asn1.schema",   schemaPath)
      .option("asn1.type",     "SimpleRecord")
      .option("asn1.encoding", "per-aligned")
      .option("asn1.per.framing", "length-prefixed")
      .load(writePerFile("aper_empty.per", Nil).getAbsolutePath)

    val sparkSchema    = df0.schema
    val toInternal     = CatalystTypeConverters.createToCatalystConverter(sparkSchema)

    val rows = Seq(
      Row(1L,  "Alice", true,  95L, Array[Byte](0x01, 0x02, 0x03), "1.2.840"),
      Row(2L,  "Bob",   false, 42L, Array[Byte](0x04, 0x05),       "1.3.6.1"),
      Row(3L,  "Carol", true,  78L, Array[Byte](0xAA.toByte),      "2.16.840.1")
    )

    val perRecords = rows.map { row =>
      encoder.encodeRow(toInternal(row).asInstanceOf[InternalRow], sparkSchema, rootType)
    }

    val df = spark.read
      .format("asn1")
      .option("asn1.schema",   schemaPath)
      .option("asn1.type",     "SimpleRecord")
      .option("asn1.encoding", "per-aligned")
      .option("asn1.per.framing", "length-prefixed")
      .load(writePerFile("aper_simple.per", perRecords).getAbsolutePath)

    val expected = Seq(
      Seq(1L,  "Alice", true,  95L, Seq[Byte](0x01, 0x02, 0x03), "1.2.840"),
      Seq(2L,  "Bob",   false, 42L, Seq[Byte](0x04, 0x05),       "1.3.6.1"),
      Seq(3L,  "Carol", true,  78L, Seq[Byte](0xAA.toByte),      "2.16.840.1")
    )

    checkAnswer(df, expected, sortCol = "name")
  }

  it should "return only requested columns when APER file is column-pruned" in {
    val schemaPath = new File(schemaDir, "simple.asn1").getAbsolutePath
    val registry   = SchemaCache.getOrParse(Seq(schemaPath))
    val modName    = registry.modules.keys.head
    val rootType   = registry.resolve("SimpleRecord", modName).get
    val encoder    = new AlignedPerEncoder(registry, modName)

    val df0 = spark.read
      .format("asn1")
      .option("asn1.schema",   schemaPath)
      .option("asn1.type",     "SimpleRecord")
      .option("asn1.encoding", "per-aligned")
      .option("asn1.per.framing", "length-prefixed")
      .load(writePerFile("aper_prune_empty.per", Nil).getAbsolutePath)

    val sparkSchema = df0.schema
    val toInternal  = CatalystTypeConverters.createToCatalystConverter(sparkSchema)
    val rows = Seq(
      Row(10L, "Zara",  true,  88L, Array[Byte](0xFF.toByte), "1.2.3"),
      Row(20L, "Yusuf", false, 55L, Array[Byte](0x00),        "1.2.4")
    )
    val perRecords = rows.map(r => encoder.encodeRow(toInternal(r).asInstanceOf[InternalRow], sparkSchema, rootType))

    val df = spark.read
      .format("asn1")
      .option("asn1.schema",   schemaPath)
      .option("asn1.type",     "SimpleRecord")
      .option("asn1.encoding", "per-aligned")
      .option("asn1.per.framing", "length-prefixed")
      .load(writePerFile("aper_pruned.per", perRecords).getAbsolutePath)
      .select("id", "name")

    df.schema.fieldNames.toSeq shouldBe Seq("id", "name")
    df.count() shouldBe 2

    val result = df.collect().sortBy(_.getLong(0))
    result(0).getLong(0)   shouldBe 10L
    result(0).getString(1) shouldBe "Zara"
    result(1).getLong(0)   shouldBe 20L
    result(1).getString(1) shouldBe "Yusuf"
  }

  // =========================================================================
  // UPER — Unaligned PER
  // =========================================================================

  it should "read a UPER file (length-prefixed) with SimpleRecord schema" in {
    val schemaPath = new File(schemaDir, "simple.asn1").getAbsolutePath
    val registry   = SchemaCache.getOrParse(Seq(schemaPath))
    val modName    = registry.modules.keys.head
    val rootType   = registry.resolve("SimpleRecord", modName).get
    val encoder    = new UnalignedPerEncoder(registry, modName)

    val df0 = spark.read
      .format("asn1")
      .option("asn1.schema",   schemaPath)
      .option("asn1.type",     "SimpleRecord")
      .option("asn1.encoding", "per-unaligned")
      .option("asn1.per.framing", "length-prefixed")
      .load(writePerFile("uper_empty.per", Nil).getAbsolutePath)

    val sparkSchema = df0.schema
    val toInternal  = CatalystTypeConverters.createToCatalystConverter(sparkSchema)
    val rows = Seq(
      Row(5L, "Eve", true, 50L, Array[Byte](0xAB.toByte, 0xCD.toByte), "1.3.6"),
      Row(6L, "Frank", false, 0L, Array[Byte](0x00), "2.5.4")
    )
    val perRecords = rows.map(r => encoder.encodeRow(toInternal(r).asInstanceOf[InternalRow], sparkSchema, rootType))

    val df = spark.read
      .format("asn1")
      .option("asn1.schema",   schemaPath)
      .option("asn1.type",     "SimpleRecord")
      .option("asn1.encoding", "per-unaligned")
      .option("asn1.per.framing", "length-prefixed")
      .load(writePerFile("uper_simple.per", perRecords).getAbsolutePath)

    val expected = Seq(
      Seq(5L, "Eve",   true,  50L, Seq[Byte](0xAB.toByte, 0xCD.toByte), "1.3.6"),
      Seq(6L, "Frank", false,  0L, Seq[Byte](0x00),                      "2.5.4")
    )
    checkAnswer(df, expected, sortCol = "name")
  }

  // =========================================================================
  // XER
  // =========================================================================

  it should "read an XER file with SimpleRecord schema" in {
    val schemaPath = new File(schemaDir, "simple.asn1").getAbsolutePath
    val registry   = SchemaCache.getOrParse(Seq(schemaPath))
    val modName    = registry.modules.keys.head
    val rootType   = registry.resolve("SimpleRecord", modName).get
    val encoder    = new XerEncoder(registry, modName)

    val df0 = spark.read
      .format("asn1")
      .option("asn1.schema",   schemaPath)
      .option("asn1.type",     "SimpleRecord")
      .option("asn1.encoding", "xer")
      .load(writeXerFile("xer_empty.xer", Nil).getAbsolutePath)

    val sparkSchema = df0.schema
    val toInternal  = CatalystTypeConverters.createToCatalystConverter(sparkSchema)
    val rows = Seq(
      Row(1L,  "Alice", true,  95L, Array[Byte](0x01, 0x02, 0x03), "1.2.840"),
      Row(2L,  "Bob",   false, 42L, Array[Byte](0x04, 0x05),       "1.3.6.1")
    )
    val xerRecords = rows.map(r => encoder.encodeRow(
      toInternal(r).asInstanceOf[InternalRow], sparkSchema, rootType, "SimpleRecord"))

    val df = spark.read
      .format("asn1")
      .option("asn1.schema",   schemaPath)
      .option("asn1.type",     "SimpleRecord")
      .option("asn1.encoding", "xer")
      .load(writeXerFile("xer_simple.xer", xerRecords).getAbsolutePath)

    val expected = Seq(
      Seq(1L, "Alice", true,  95L, Seq[Byte](0x01, 0x02, 0x03), "1.2.840"),
      Seq(2L, "Bob",   false, 42L, Seq[Byte](0x04, 0x05),       "1.3.6.1")
    )
    checkAnswer(df, expected, sortCol = "name")
  }

  it should "return only requested columns when XER file is column-pruned" in {
    val schemaPath = new File(schemaDir, "simple.asn1").getAbsolutePath
    val registry   = SchemaCache.getOrParse(Seq(schemaPath))
    val modName    = registry.modules.keys.head
    val rootType   = registry.resolve("SimpleRecord", modName).get
    val encoder    = new XerEncoder(registry, modName)

    val df0 = spark.read
      .format("asn1")
      .option("asn1.schema",   schemaPath)
      .option("asn1.type",     "SimpleRecord")
      .option("asn1.encoding", "xer")
      .load(writeXerFile("xer_prune_empty.xer", Nil).getAbsolutePath)

    val sparkSchema = df0.schema
    val toInternal  = CatalystTypeConverters.createToCatalystConverter(sparkSchema)
    val rows = Seq(
      Row(7L, "Gina", true, 60L, Array[Byte](0x01), "1.2.3"),
      Row(8L, "Hugo", false, 30L, Array[Byte](0x02), "1.2.4")
    )
    val xerRecords = rows.map(r => encoder.encodeRow(
      toInternal(r).asInstanceOf[InternalRow], sparkSchema, rootType, "SimpleRecord"))

    val df = spark.read
      .format("asn1")
      .option("asn1.schema",   schemaPath)
      .option("asn1.type",     "SimpleRecord")
      .option("asn1.encoding", "xer")
      .load(writeXerFile("xer_pruned.xer", xerRecords).getAbsolutePath)
      .select("id", "name")

    df.schema.fieldNames.toSeq shouldBe Seq("id", "name")
    df.count() shouldBe 2

    val result = df.collect().sortBy(_.getLong(0))
    result(0).getLong(0)   shouldBe 7L
    result(0).getString(1) shouldBe "Gina"
    result(1).getLong(0)   shouldBe 8L
    result(1).getString(1) shouldBe "Hugo"
  }

  // =========================================================================
  // asn1.enumerated.as.int — XER and APER round-trip
  // =========================================================================

  it should "decode ENUMERATED as String (default) for XER NestedRecord" in {
    val schemaPath = new File(schemaDir, "nested.asn1").getAbsolutePath

    // Hand-craft a minimal XER record: only id + status (skip optional BIT STRING / SEQUENCE OF)
    val xml =
      "<NestedRecord>" +
        "<id>7</id>" +
        "<status><inactive/></status>" +
        "<permissions>A0</permissions>" +
        "<tags/>" +
        "<labels/>" +
      "</NestedRecord>\n"

    val f   = new File(tmpDir.toFile, "xer_nested_enum_str.xer")
    val out = new FileOutputStream(f)
    try out.write(xml.getBytes("UTF-8"))
    finally out.close()

    val df = spark.read
      .format("asn1")
      .option("asn1.schema",   schemaPath)
      .option("asn1.type",     "NestedRecord")
      .option("asn1.encoding", "xer")
      .load(f.getAbsolutePath)

    df.count() shouldBe 1
    val row = df.collect().head
    row.getString(df.schema.fieldIndex("status")) shouldBe "inactive"
  }

  it should "decode ENUMERATED as Long when asn1.enumerated.as.int=true for XER NestedRecord" in {
    val schemaPath = new File(schemaDir, "nested.asn1").getAbsolutePath

    val xml =
      "<NestedRecord>" +
        "<id>7</id>" +
        "<status><inactive/></status>" +
        "<permissions>A0</permissions>" +
        "<tags/>" +
        "<labels/>" +
      "</NestedRecord>\n"

    val f   = new File(tmpDir.toFile, "xer_nested_enum_int.xer")
    val out = new FileOutputStream(f)
    try out.write(xml.getBytes("UTF-8"))
    finally out.close()

    val df = spark.read
      .format("asn1")
      .option("asn1.schema",          schemaPath)
      .option("asn1.type",            "NestedRecord")
      .option("asn1.encoding",        "xer")
      .option("asn1.enumerated.as.int", "true")
      .load(f.getAbsolutePath)

    df.count() shouldBe 1
    val row = df.collect().head
    // inactive has value 1 in the schema
    row.getLong(df.schema.fieldIndex("status")) shouldBe 1L
  }

  it should "decode ENUMERATED as Long when asn1.enumerated.as.int=true for APER NestedRecord" in {
    val schemaPath = new File(schemaDir, "nested.asn1").getAbsolutePath
    val registry   = SchemaCache.getOrParse(Seq(schemaPath))
    val modName    = registry.modules.keys.head
    val rootType   = registry.resolve("NestedRecord", modName).get

    // Build schema with enumAsInt=false first to create InternalRow, then re-encode
    val df0 = spark.read
      .format("asn1")
      .option("asn1.schema",   schemaPath)
      .option("asn1.type",     "NestedRecord")
      .option("asn1.encoding", "per-aligned")
      .option("asn1.per.framing", "length-prefixed")
      .load(writePerFile("aper_enum_empty.per", Nil).getAbsolutePath)

    // Manually encode a NestedRecord using the encoder with enumAsInt=true schema
    // by using the Long-typed schema directly.
    import io.github.sparkasn1.spark.asn1.codec.per.AlignedPerEncoder
    import io.github.sparkasn1.spark.asn1.schema.Asn1TypeMapper
    import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
    import org.apache.spark.sql.catalyst.util.GenericArrayData

    val enumAsIntSchema = Asn1TypeMapper.toSparkType(rootType, registry, modName,
      enumeratedAsInt = true).asInstanceOf[org.apache.spark.sql.types.StructType]
    val encoder = new AlignedPerEncoder(registry, modName)

    // status=1 (inactive), permissions=[0xA0], tags=[], labels=[]
    val permBytes  = new GenericInternalRow(Array[Any](
      Array[Byte](0xA0.toByte),
      new GenericArrayData(Array.empty[Any])
    ))
    val testRow = new GenericInternalRow(Array[Any](
      9L,                               // id
      1L,                               // status as int (inactive=1)
      permBytes,                        // permissions struct
      new GenericArrayData(Array.empty[Any]),  // tags
      new GenericArrayData(Array.empty[Any])   // labels
    ))

    val perBytes = encoder.encodeRow(testRow, enumAsIntSchema, rootType)

    val df = spark.read
      .format("asn1")
      .option("asn1.schema",            schemaPath)
      .option("asn1.type",              "NestedRecord")
      .option("asn1.encoding",          "per-aligned")
      .option("asn1.per.framing",       "length-prefixed")
      .option("asn1.enumerated.as.int", "true")
      .load(writePerFile("aper_enum_int.per", Seq(perBytes)).getAbsolutePath)

    df.count() shouldBe 1
    val row = df.collect().head
    row.getLong(df.schema.fieldIndex("id"))     shouldBe 9L
    row.getLong(df.schema.fieldIndex("status")) shouldBe 1L
  }
}
