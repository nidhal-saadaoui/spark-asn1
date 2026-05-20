package io.github.sparkasn1.spark.asn1.integration

import io.github.sparkasn1.spark.asn1.codec.BerDerEncoder
import io.github.sparkasn1.spark.asn1.util.SchemaCache
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.bouncycastle.asn1._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

import java.io.{File, FileOutputStream}
import java.nio.file.{Files, Path}

class DataFrameReadSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _
  private var tmpDir: Path        = _
  private val schemaDir = new File(getClass.getResource("/schemas").getPath)

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .master("local[2]")
      .appName("spark-asn1-df-read-test")
      .config("spark.ui.enabled", "false")
      .config("spark.hadoop.fs.file.impl.disable.cache", "true")
      .config("spark.serializer.extraDebugInfo", "false")
      .getOrCreate()
    tmpDir = Files.createTempDirectory("spark-asn1-df-test")
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
    deleteRecursively(tmpDir.toFile)
  }

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  private def writeBer(name: String, records: Seq[Array[Byte]]): File = {
    val f = new File(tmpDir.toFile, name)
    val out = new FileOutputStream(f)
    try records.foreach(out.write)
    finally out.close()
    f
  }

  private def der(obj: ASN1Primitive): Array[Byte] = obj.getEncoded("DER")

  private def deleteRecursively(f: File): Unit = {
    if (f.isDirectory) f.listFiles().foreach(deleteRecursively)
    f.delete()
  }

  /**
   * Compare the whole DataFrame against an expected row sequence.
   * Array[Byte] fields are normalised to Seq[Byte] so structural equality works.
   * Both sides are sorted by `sortCol` before comparison.
   */
  private def checkAnswer(
    df:      org.apache.spark.sql.DataFrame,
    expected: Seq[Seq[Any]],
    sortCol:  String
  ): Unit = {
    def normalise(row: Row): Seq[Any] =
      (0 until row.size).map(i => row.get(i) match {
        case b: Array[Byte] => b.toSeq
        case v              => v
      })

    // Collect then sort in Scala to avoid df.orderBy() which triggers RangePartitioner
    // and loads sun.security.action.GetBooleanAction (removed in Java 25).
    val keyIdx  = df.schema.fieldIndex(sortCol)
    val actual  = df.collect().map(normalise).toSeq
      .sortWith { (a, b) => a(keyIdx).asInstanceOf[Comparable[Any]].compareTo(b(keyIdx)) < 0 }
    val exp     = expected.sortWith { (a, b) =>
      a(keyIdx).asInstanceOf[Comparable[Any]].compareTo(b(keyIdx)) < 0
    }

    actual.length shouldBe exp.length
    actual.zip(exp).zipWithIndex.foreach { case ((a, e), i) =>
      withClue(s"Row $i: actual=$a  expected=$e\n") { a shouldBe e }
    }
  }

  // -------------------------------------------------------------------------
  // SimpleRecord — all primitive types, whole-DataFrame comparison
  // -------------------------------------------------------------------------

  "Asn1DataSource" should "return the correct DataFrame for SimpleRecord BER file" in {

    val records = Seq(
      der(new DERSequence(Array[ASN1Encodable](
        new ASN1Integer(1), new DERUTF8String("Alice"),
        ASN1Boolean.getInstance(true),  new ASN1Integer(95),
        new DEROctetString(Array[Byte](0x01, 0x02, 0x03)),
        new ASN1ObjectIdentifier("1.2.840")
      ))),
      der(new DERSequence(Array[ASN1Encodable](
        new ASN1Integer(2), new DERUTF8String("Bob"),
        ASN1Boolean.getInstance(false), new ASN1Integer(42),
        new DEROctetString(Array[Byte](0x04, 0x05)),
        new ASN1ObjectIdentifier("1.3.6.1")
      ))),
      der(new DERSequence(Array[ASN1Encodable](
        new ASN1Integer(3), new DERUTF8String("Carol"),
        ASN1Boolean.getInstance(true),  new ASN1Integer(78),
        new DEROctetString(Array[Byte](0xAA.toByte)),
        new ASN1ObjectIdentifier("2.16.840.1")
      )))
    )

    val df = spark.read
      .format("asn1")
      .option("asn1.schema",   new File(schemaDir, "simple.asn1").getAbsolutePath)
      .option("asn1.type",     "SimpleRecord")
      .option("asn1.encoding", "ber")
      .load(writeBer("simple_full.ber", records).getAbsolutePath)

    // id | name  | active | score | data (Seq[Byte]) | oid
    val expected = Seq(
      Seq(1L,  "Alice", true,  95L, Seq[Byte](0x01, 0x02, 0x03), "1.2.840"),
      Seq(2L,  "Bob",   false, 42L, Seq[Byte](0x04, 0x05),       "1.3.6.1"),
      Seq(3L,  "Carol", true,  78L, Seq[Byte](0xAA.toByte),      "2.16.840.1")
    )

    checkAnswer(df, expected, sortCol = "name")
  }

  // -------------------------------------------------------------------------
  // PersonRecord — OPTIONAL fields (null handling), whole-DataFrame comparison
  // -------------------------------------------------------------------------

  it should "return correct nulls for OPTIONAL fields in PersonRecord BER file" in {

    // EXPLICIT context tags [0]..[3] so the decoder uses tag-based alignment
    // and can distinguish absent optional fields regardless of element count.
    def personDer(firstName: String, lastName: String,
                  age: Option[Int], email: Option[String]): Array[Byte] = {
      val elems = Seq[ASN1Encodable](
        new DERTaggedObject(true, 0, new DERUTF8String(firstName)),
        new DERTaggedObject(true, 1, new DERUTF8String(lastName))
      ) ++
        age.map(a => new DERTaggedObject(true, 2, new ASN1Integer(a)): ASN1Encodable) ++
        email.map(e => new DERTaggedObject(true, 3, new DERUTF8String(e)): ASN1Encodable)
      der(new DERSequence(elems.toArray))
    }

    val records = Seq(
      personDer("Alice", "Smith", Some(30), Some("alice@example.com")),
      personDer("Bob",   "Jones", None,     None),
      personDer("Carol", "White", Some(25), None),
      personDer("Dave",  "Brown", None,     Some("dave@example.com"))
    )

    val df = spark.read
      .format("asn1")
      .option("asn1.schema",   new File(schemaDir, "simple.asn1").getAbsolutePath)
      .option("asn1.type",     "PersonRecord")
      .option("asn1.encoding", "ber")
      .load(writeBer("person_optional.ber", records).getAbsolutePath)

    // firstName | lastName | age  | email
    val expected = Seq(
      Seq("Alice", "Smith", 30L,  "alice@example.com"),
      Seq("Bob",   "Jones", null, null),
      Seq("Carol", "White", 25L,  null),
      Seq("Dave",  "Brown", null, "dave@example.com")
    )

    checkAnswer(df, expected, sortCol = "firstName")
  }

  // -------------------------------------------------------------------------
  // IMPLICIT tagging — context tag replaces inner type tag on the wire
  // -------------------------------------------------------------------------

  it should "decode IMPLICIT context-tagged OPTIONAL fields correctly" in {

    // IMPLICIT tags: [n] replaces the inner universal tag on the wire.
    // The decoder must reconstruct the correct typed primitive from the expected schema type.
    def personImplicit(firstName: String, lastName: String,
                       age: Option[Int], email: Option[String]): Array[Byte] = {
      val elems = Seq[ASN1Encodable](
        new DERTaggedObject(false, 0, new DERUTF8String(firstName)),
        new DERTaggedObject(false, 1, new DERUTF8String(lastName))
      ) ++
        age.map(a => new DERTaggedObject(false, 2, new ASN1Integer(a)): ASN1Encodable) ++
        email.map(e => new DERTaggedObject(false, 3, new DERUTF8String(e)): ASN1Encodable)
      der(new DERSequence(elems.toArray))
    }

    val records = Seq(
      personImplicit("Alice", "Smith", Some(30), Some("alice@example.com")),
      personImplicit("Bob",   "Jones", None,     None),
      personImplicit("Dave",  "Brown", None,     Some("dave@example.com"))
    )

    val df = spark.read
      .format("asn1")
      .option("asn1.schema",   new File(schemaDir, "simple.asn1").getAbsolutePath)
      .option("asn1.type",     "PersonRecord")
      .option("asn1.encoding", "ber")
      .load(writeBer("person_implicit.ber", records).getAbsolutePath)

    val expected = Seq(
      Seq("Alice", "Smith", 30L,  "alice@example.com"),
      Seq("Bob",   "Jones", null, null),
      Seq("Dave",  "Brown", null, "dave@example.com")
    )

    checkAnswer(df, expected, sortCol = "firstName")
  }

  // -------------------------------------------------------------------------
  // Column pruning — SELECT a subset of fields
  // -------------------------------------------------------------------------

  it should "return only requested columns when pruned (SELECT id, name)" in {

    val records = Seq(
      der(new DERSequence(Array[ASN1Encodable](
        new ASN1Integer(10), new DERUTF8String("Zara"),
        ASN1Boolean.getInstance(true), new ASN1Integer(88),
        new DEROctetString(Array[Byte](0xFF.toByte)),
        new ASN1ObjectIdentifier("1.2.3")
      ))),
      der(new DERSequence(Array[ASN1Encodable](
        new ASN1Integer(20), new DERUTF8String("Yusuf"),
        ASN1Boolean.getInstance(false), new ASN1Integer(55),
        new DEROctetString(Array[Byte](0x00)),
        new ASN1ObjectIdentifier("1.2.4")
      )))
    )

    val df = spark.read
      .format("asn1")
      .option("asn1.schema",   new File(schemaDir, "simple.asn1").getAbsolutePath)
      .option("asn1.type",     "SimpleRecord")
      .option("asn1.encoding", "ber")
      .load(writeBer("simple_pruned.ber", records).getAbsolutePath)
      .select("id", "name")   // triggers column pruning

    df.schema.fieldNames.toSeq shouldBe Seq("id", "name")
    df.count() shouldBe 2

    val rows = df.orderBy("id").collect()
    rows(0).getLong(0)   shouldBe 10L
    rows(0).getString(1) shouldBe "Zara"
    rows(1).getLong(0)   shouldBe 20L
    rows(1).getString(1) shouldBe "Yusuf"
  }

  // -------------------------------------------------------------------------
  // BER write — encode a DataFrame and read it back
  // -------------------------------------------------------------------------

  it should "write a DataFrame as BER and read it back correctly" in {

    val schemaPath = new File(schemaDir, "simple.asn1").getAbsolutePath

    val records = Seq(
      der(new DERSequence(Array[ASN1Encodable](
        new ASN1Integer(1), new DERUTF8String("Alice"),
        ASN1Boolean.getInstance(true),  new ASN1Integer(95),
        new DEROctetString(Array[Byte](0x01, 0x02)),
        new ASN1ObjectIdentifier("1.2.3")
      ))),
      der(new DERSequence(Array[ASN1Encodable](
        new ASN1Integer(2), new DERUTF8String("Bob"),
        ASN1Boolean.getInstance(false), new ASN1Integer(42),
        new DEROctetString(Array[Byte](0x04)),
        new ASN1ObjectIdentifier("1.3.6")
      )))
    )

    val sourceFile = writeBer("write_source.ber", records)

    val dfIn = spark.read
      .format("asn1")
      .option("asn1.schema",   schemaPath)
      .option("asn1.type",     "SimpleRecord")
      .option("asn1.encoding", "ber")
      .load(sourceFile.getAbsolutePath)

    // Encode directly (bypass Hadoop FileFormatWriter, which fails on Java 25)
    val registry  = SchemaCache.getOrParse(Seq(schemaPath))
    val modName   = registry.modules.keys.head
    val rootType  = registry.resolve("SimpleRecord", modName).get
    val encoder   = new BerDerEncoder(registry, modName)
    val sparkSch  = dfIn.schema
    val toInternal = CatalystTypeConverters.createToCatalystConverter(sparkSch)
    val outFile   = new File(tmpDir.toFile, "ber_write_out.ber")
    val fos       = new FileOutputStream(outFile)
    try dfIn.collect()
      .foreach(row => fos.write(encoder.encodeRow(toInternal(row).asInstanceOf[InternalRow], sparkSch, rootType)))
    finally fos.close()

    val dfOut = spark.read
      .format("asn1")
      .option("asn1.schema",   schemaPath)
      .option("asn1.type",     "SimpleRecord")
      .option("asn1.encoding", "ber")
      .load(outFile.getAbsolutePath)

    val expected = Seq(
      Seq(1L,  "Alice", true,  95L, Seq[Byte](0x01, 0x02), "1.2.3"),
      Seq(2L,  "Bob",   false, 42L, Seq[Byte](0x04),       "1.3.6")
    )

    checkAnswer(dfOut, expected, sortCol = "name")
  }

  it should "write OPTIONAL fields (including nulls) as BER and read back" in {

    val schemaPath = new File(schemaDir, "simple.asn1").getAbsolutePath

    def personDerTagged(firstName: String, lastName: String,
                        age: Option[Int], email: Option[String]): Array[Byte] = {
      val elems = Seq[ASN1Encodable](
        new DERTaggedObject(true, 0, new DERUTF8String(firstName)),
        new DERTaggedObject(true, 1, new DERUTF8String(lastName))
      ) ++
        age.map(a   => new DERTaggedObject(true, 2, new ASN1Integer(a)):   ASN1Encodable) ++
        email.map(e => new DERTaggedObject(true, 3, new DERUTF8String(e)): ASN1Encodable)
      der(new DERSequence(elems.toArray))
    }

    val sourceFile = writeBer("write_person_src.ber", Seq(
      personDerTagged("Alice", "Smith", Some(30), Some("alice@example.com")),
      personDerTagged("Bob",   "Jones", None,     None)
    ))

    val dfIn = spark.read
      .format("asn1")
      .option("asn1.schema",   schemaPath)
      .option("asn1.type",     "PersonRecord")
      .option("asn1.encoding", "ber")
      .load(sourceFile.getAbsolutePath)

    val registry   = SchemaCache.getOrParse(Seq(schemaPath))
    val modName    = registry.modules.keys.head
    val rootType   = registry.resolve("PersonRecord", modName).get
    val encoder    = new BerDerEncoder(registry, modName)
    val sparkSch   = dfIn.schema
    val toInternal = CatalystTypeConverters.createToCatalystConverter(sparkSch)
    val outFile    = new File(tmpDir.toFile, "person_write_out.ber")
    val fos        = new FileOutputStream(outFile)
    try dfIn.collect()
      .foreach(row => fos.write(encoder.encodeRow(toInternal(row).asInstanceOf[InternalRow], sparkSch, rootType)))
    finally fos.close()

    val dfOut = spark.read
      .format("asn1")
      .option("asn1.schema",   schemaPath)
      .option("asn1.type",     "PersonRecord")
      .option("asn1.encoding", "ber")
      .load(outFile.getAbsolutePath)

    val expected = Seq(
      Seq("Alice", "Smith", 30L, "alice@example.com"),
      Seq("Bob",   "Jones", null, null)
    )

    checkAnswer(dfOut, expected, sortCol = "firstName")
  }

  // -------------------------------------------------------------------------
  // Schema — verify inferred StructType matches expected field types
  // -------------------------------------------------------------------------

  it should "infer the correct Spark schema for PersonRecord" in {
    val singleRecord = der(new DERSequence(Array[ASN1Encodable](
      new DERUTF8String("X"), new DERUTF8String("Y")
    )))

    val schema = spark.read
      .format("asn1")
      .option("asn1.schema",   new File(schemaDir, "simple.asn1").getAbsolutePath)
      .option("asn1.type",     "PersonRecord")
      .option("asn1.encoding", "ber")
      .load(writeBer("person_schema_check.ber", Seq(singleRecord)).getAbsolutePath)
      .schema

    schema("firstName").dataType shouldBe StringType
    schema("lastName").dataType  shouldBe StringType
    schema("age").dataType       shouldBe LongType
    schema("age").nullable       shouldBe true
    schema("email").dataType     shouldBe StringType
    schema("email").nullable     shouldBe true
  }
}
