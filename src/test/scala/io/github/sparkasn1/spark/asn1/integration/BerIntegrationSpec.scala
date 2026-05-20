package io.github.sparkasn1.spark.asn1.integration

import org.apache.spark.sql.SparkSession
import org.bouncycastle.asn1._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import java.io.{File, FileOutputStream}
import java.nio.file.{Files, Path}

/**
 * Integration tests: encode BER data with BouncyCastle, write to a temp file,
 * then read it back via the Asn1DataSource and assert on the results.
 */
class BerIntegrationSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _
  private var tmpDir: Path        = _
  private val schemaDir = new File(getClass.getResource("/schemas").getPath)

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .master("local[2]")
      .appName("spark-asn1-integration-test")
      .config("spark.ui.enabled", "false")
      .config("spark.hadoop.fs.file.impl.disable.cache", "true")
      // sun.security.action.GetBooleanAction removed in Java 25; disable debug to avoid loading it.
      .config("spark.serializer.extraDebugInfo", "false")
      .getOrCreate()
    tmpDir = Files.createTempDirectory("spark-asn1-test")
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
    deleteRecursively(tmpDir.toFile)
  }

  // -----------------------------------------------------------------------
  // Helpers
  // -----------------------------------------------------------------------

  private def berFile(name: String, records: Seq[Array[Byte]]): File = {
    val f   = new File(tmpDir.toFile, name)
    val out = new FileOutputStream(f)
    try records.foreach(out.write)
    finally out.close()
    f
  }

  private def encodeDer(obj: ASN1Primitive): Array[Byte] = obj.getEncoded("DER")

  private def deleteRecursively(f: File): Unit = {
    if (f.isDirectory) f.listFiles().foreach(deleteRecursively)
    f.delete()
  }

  // -----------------------------------------------------------------------
  // Tests
  // -----------------------------------------------------------------------

  "Asn1DataSource" should "read a BER file with SimpleRecord schema" in {
    val rec1 = encodeDer(new DERSequence(Array[ASN1Encodable](
      new ASN1Integer(1),
      new DERUTF8String("Alice"),
      ASN1Boolean.getInstance(true),
      new ASN1Integer(95),
      new DEROctetString(Array[Byte](1, 2, 3)),
      new ASN1ObjectIdentifier("1.2.3")
    )))
    val rec2 = encodeDer(new DERSequence(Array[ASN1Encodable](
      new ASN1Integer(2),
      new DERUTF8String("Bob"),
      ASN1Boolean.getInstance(false),
      new ASN1Integer(42),
      new DEROctetString(Array[Byte](4, 5)),
      new ASN1ObjectIdentifier("1.2.3.4")
    )))

    val dataFile = berFile("simple_records.ber", Seq(rec1, rec2))
    val schemaPath = new File(schemaDir, "simple.asn1").getAbsolutePath

    val df = spark.read
      .format("asn1")
      .option("asn1.schema", schemaPath)
      .option("asn1.type", "SimpleRecord")
      .option("asn1.encoding", "ber")
      .load(dataFile.getAbsolutePath)

    df.count() shouldBe 2

    val rows = df.collect()
    rows(0).getLong(df.schema.fieldIndex("id"))       shouldBe 1L
    rows(0).getString(df.schema.fieldIndex("name"))   shouldBe "Alice"
    rows(0).getBoolean(df.schema.fieldIndex("active")) shouldBe true
    rows(0).getLong(df.schema.fieldIndex("score"))    shouldBe 95L

    rows(1).getLong(df.schema.fieldIndex("id"))       shouldBe 2L
    rows(1).getString(df.schema.fieldIndex("name"))   shouldBe "Bob"
    rows(1).getBoolean(df.schema.fieldIndex("active")) shouldBe false
  }

  it should "read a BER file with SEQUENCE OF" in {
    val tag1 = new DERSequence(Array[ASN1Encodable](
      new DERUTF8String("env"), new DERUTF8String("prod")))
    val tag2 = new DERSequence(Array[ASN1Encodable](
      new DERUTF8String("region"), new DERUTF8String("us-east")))
    val tags = new DERSequence(Array[ASN1Encodable](tag1, tag2))

    val labels = new DERSequence(Array[ASN1Encodable](
      new DERUTF8String("label-a"),
      new DERUTF8String("label-b")
    ))

    val record = encodeDer(new DERSequence(Array[ASN1Encodable](
      new ASN1Integer(42),
      new ASN1Enumerated(0),   // active
      new DERBitString(Array[Byte](0xa0.toByte), 5), // read + execute
      tags,
      labels
    )))

    val dataFile  = berFile("nested_record.ber", Seq(record))
    val schemaPath = new File(schemaDir, "nested.asn1").getAbsolutePath

    val df = spark.read
      .format("asn1")
      .option("asn1.schema", schemaPath)
      .option("asn1.type", "NestedRecord")
      .option("asn1.encoding", "ber")
      .load(dataFile.getAbsolutePath)

    df.count() shouldBe 1
    val row = df.collect().head
    row.getLong(df.schema.fieldIndex("id")) shouldBe 42L
  }

  it should "infer schema correctly" in {
    val schemaPath = new File(schemaDir, "simple.asn1").getAbsolutePath
    val rec = encodeDer(new DERSequence(Array[ASN1Encodable](
      new ASN1Integer(1), new DERUTF8String("x"),
      ASN1Boolean.getInstance(true), new ASN1Integer(1),
      new DEROctetString(Array[Byte](0)), new ASN1ObjectIdentifier("1.2")
    )))
    val dataFile = berFile("infer_schema.ber", Seq(rec))

    val df = spark.read
      .format("asn1")
      .option("asn1.schema", schemaPath)
      .option("asn1.type", "SimpleRecord")
      .load(dataFile.getAbsolutePath)

    val schema = df.schema
    schema.fieldNames should contain allOf("id", "name", "active", "score", "data", "oid")
  }
}
