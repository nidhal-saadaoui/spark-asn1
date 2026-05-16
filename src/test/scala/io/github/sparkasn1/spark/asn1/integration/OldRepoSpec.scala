package io.github.sparkasn1.spark.asn1.integration

import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

import java.io.File

/**
 * Integration tests using the original BER files and schemas from
 * https://github.com/SNidhal/ASN1SparkDatasource (the predecessor project).
 *
 * Covers two cases not exercised by the main suite:
 *   - APPLICATION class tags (telecom CDR records in cdr.ber / cdr.asn1)
 *   - Nested inline SEQUENCE under AUTOMATIC TAGS (human.ber / human.asn1)
 */
class OldRepoSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _
  private val schemaDir = new File(getClass.getResource("/schemas").getPath)
  private val dataDir   = new File(getClass.getResource("/data").getPath)

  override def beforeAll(): Unit =
    spark = SparkSession.builder()
      .master("local[2]")
      .appName("old-repo-spec")
      .config("spark.ui.enabled", "false")
      .config("spark.hadoop.fs.file.impl.disable.cache", "true")
      .getOrCreate()

  override def afterAll(): Unit =
    if (spark != null) spark.stop()

  // -------------------------------------------------------------------------
  // CDR — APPLICATION class tags (simpleTypes.ber from old repo)
  // -------------------------------------------------------------------------

  "Asn1DataSource" should "decode CDR records with APPLICATION class tags" in {
    val df = spark.read
      .format("asn1")
      .option("asn1.schema",   new File(schemaDir, "cdr.asn1").getAbsolutePath)
      .option("asn1.type",     "GenericCallDataRecord")
      .option("asn1.encoding", "ber")
      .load(new File(dataDir, "cdr.ber").getAbsolutePath)

    df.count() shouldBe 5

    val rows = df.orderBy("recordNumber").collect()

    // Record 1
    rows(0).getLong(0)   shouldBe 1L
    rows(0).getString(1) shouldBe "15555550100"
    rows(0).getString(2) shouldBe "15555550101"
    rows(0).getString(3) shouldBe "20131016"
    rows(0).getString(4) shouldBe "134534"
    rows(0).getLong(5)   shouldBe 65L

    // Record 2
    rows(1).getLong(0)   shouldBe 2L
    rows(1).getString(1) shouldBe "15555550102"
    rows(1).getString(2) shouldBe "15555550104"
    rows(1).getLong(5)   shouldBe 52L

    // Record 3
    rows(2).getLong(0) shouldBe 3L
    rows(2).getLong(5) shouldBe 62L

    // Record 4
    rows(3).getLong(0) shouldBe 4L
    rows(3).getLong(5) shouldBe 72L

    // Record 5
    rows(4).getLong(0)   shouldBe 5L
    rows(4).getString(1) shouldBe "15555550101"
    rows(4).getString(2) shouldBe "15555550100"
    rows(4).getLong(5)   shouldBe 32L
  }

  it should "infer the correct schema for CDR records" in {
    import org.apache.spark.sql.types._
    val schema = spark.read
      .format("asn1")
      .option("asn1.schema",   new File(schemaDir, "cdr.asn1").getAbsolutePath)
      .option("asn1.type",     "GenericCallDataRecord")
      .option("asn1.encoding", "ber")
      .load(new File(dataDir, "cdr.ber").getAbsolutePath)
      .schema

    schema("recordNumber").dataType  shouldBe LongType
    schema("callingNumber").dataType shouldBe StringType
    schema("calledNumber").dataType  shouldBe StringType
    schema("startDate").dataType     shouldBe StringType
    schema("startTime").dataType     shouldBe StringType
    schema("duration").dataType      shouldBe LongType
  }

  // -------------------------------------------------------------------------
  // Human — nested inline SEQUENCE under AUTOMATIC TAGS (nestedExample.ber)
  // -------------------------------------------------------------------------

  it should "decode a nested SEQUENCE under AUTOMATIC TAGS" in {
    import org.apache.spark.sql.types._
    val df = spark.read
      .format("asn1")
      .option("asn1.schema",   new File(schemaDir, "human.asn1").getAbsolutePath)
      .option("asn1.type",     "Human")
      .option("asn1.encoding", "ber")
      .load(new File(dataDir, "human.ber").getAbsolutePath)

    df.count() shouldBe 1

    val row = df.collect()(0)

    // foo.bar
    val foo = row.getAs[Row]("foo")
    foo.getLong(0) shouldBe 1L

    // name (optional, present)
    row.getString(row.fieldIndex("name")) shouldBe "Adam"

    // age (optional, present) — value is 256 as encoded in the original file
    row.getLong(row.fieldIndex("age")) shouldBe 256L
  }

  it should "infer the correct schema for Human records" in {
    import org.apache.spark.sql.types._
    val schema = spark.read
      .format("asn1")
      .option("asn1.schema",   new File(schemaDir, "human.asn1").getAbsolutePath)
      .option("asn1.type",     "Human")
      .option("asn1.encoding", "ber")
      .load(new File(dataDir, "human.ber").getAbsolutePath)
      .schema

    schema("foo").dataType  shouldBe a[StructType]
    schema("name").dataType shouldBe StringType
    schema("name").nullable shouldBe true
    schema("age").dataType  shouldBe LongType
    schema("age").nullable  shouldBe true
  }
}
