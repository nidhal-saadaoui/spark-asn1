package io.github.sparkasn1.spark.asn1.integration

import io.github.sparkasn1.spark.asn1.codec.BerDerEncoder
import io.github.sparkasn1.spark.asn1.schema.Asn1TypeMapper
import io.github.sparkasn1.spark.asn1.util.SchemaCache
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

import java.io.{File, FileOutputStream}
import java.nio.file.{Files, Path}

/**
 * Smoke-tests Spark structured streaming over ASN.1 BER files.
 *
 * Uses Spark's built-in FileStreamSource (which backs any FileFormat-based
 * readStream call) with a local directory source and an in-memory sink.
 * Each micro-batch writes one or more BER files to the landing directory and
 * then calls processAllAvailable() to drain the queue.
 */
class StreamingReadSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _
  private var landingDir: Path    = _
  private var checkpointDir: Path = _

  private val schemaFile  = new File(getClass.getResource("/schemas/simple.asn1").getPath)
  private val schemaPath  = schemaFile.getAbsolutePath
  private val reg         = SchemaCache.getOrParse(Seq(schemaPath))
  private val modName     = "SimpleModule"
  private val asn1Type    = reg.resolve("PersonRecord", modName).get
  private val sparkSchema =
    Asn1TypeMapper.toSparkType(asn1Type, reg, modName).asInstanceOf[StructType]
  private val encoder     = new BerDerEncoder(reg, modName)

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .master("local[2]")
      .appName("spark-asn1-streaming-test")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.streaming.schemaInference", "true")
      .config("spark.hadoop.fs.file.impl.disable.cache", "true")
      .config("spark.serializer.extraDebugInfo", "false")
      // Java 25 removed Subject.getSubject(); bypass FileContextBasedCheckpointFileManager
      .config(
        "spark.hadoop.spark.sql.streaming.checkpointFileManagerClass",
        "org.apache.spark.sql.execution.streaming.FileSystemBasedCheckpointFileManager"
      )
      .getOrCreate()
    landingDir    = Files.createTempDirectory("spark-asn1-stream-landing")
    checkpointDir = Files.createTempDirectory("spark-asn1-stream-checkpoint")
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
    deleteRecursively(landingDir.toFile)
    deleteRecursively(checkpointDir.toFile)
  }

  private def writeBerFile(name: String, rows: Seq[GenericInternalRow]): Unit = {
    val f   = new File(landingDir.toFile, name)
    val out = new FileOutputStream(f)
    try rows.foreach(r => out.write(encoder.encodeRow(r, sparkSchema, asn1Type)))
    finally out.close()
  }

  private def person(first: String, last: String, age: Option[Long], email: Option[String]) =
    new GenericInternalRow(Array[Any](
      UTF8String.fromString(first),
      UTF8String.fromString(last),
      age.orNull,
      email.map(UTF8String.fromString).orNull
    ))

  private def deleteRecursively(f: File): Unit = {
    if (f.isDirectory) f.listFiles().foreach(deleteRecursively)
    f.delete()
  }

  // -------------------------------------------------------------------------

  "readStream with asn1 format" should "process BER files as they arrive" in {
    // Write the first batch of files before the query starts
    writeBerFile("batch1a.ber", Seq(
      person("Alice", "Smith",  Some(30L), Some("alice@example.com")),
      person("Bob",   "Jones",  None,      Some("bob@example.com"))
    ))
    writeBerFile("batch1b.ber", Seq(
      person("Carol", "White", Some(25L), None)
    ))

    val query = spark.readStream
      .format("asn1")
      .option("asn1.schema",   schemaPath)
      .option("asn1.type",     "PersonRecord")
      .option("asn1.encoding", "ber")
      .schema(sparkSchema)
      .load(landingDir.toString)
      .writeStream
      .format("memory")
      .queryName("persons")
      .option("checkpointLocation", checkpointDir.toString)
      .trigger(Trigger.ProcessingTime("100 milliseconds"))
      .start()

    try {
      query.processAllAvailable()

      val result = spark.sql("select * from persons").collect()
      result should have length 3
      result.map(_.getAs[String]("firstName")).toSet shouldBe Set("Alice", "Bob", "Carol")
      result.find(_.getAs[String]("firstName") == "Alice")
        .map(_.getAs[Long]("age")) shouldBe Some(30L)
      result.find(_.getAs[String]("firstName") == "Bob")
        .map(r => Option(r.getAs[String]("email"))) shouldBe Some(Some("bob@example.com"))

      // Second batch: two more files arrive
      writeBerFile("batch2a.ber", Seq(person("Dave",  "Brown", Some(40L), None)))
      writeBerFile("batch2b.ber", Seq(person("Eve",   "Green", None,      Some("eve@example.com"))))

      query.processAllAvailable()

      val result2 = spark.sql("select * from persons").collect()
      result2 should have length 5
      result2.map(_.getAs[String]("firstName")).toSet shouldBe
        Set("Alice", "Bob", "Carol", "Dave", "Eve")

    } finally {
      query.stop()
    }
  }
}
