package io.github.sparkasn1.spark.asn1.util

import org.bouncycastle.asn1._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.{File, FileOutputStream}
import java.nio.file.Files

class Asn1InspectorSpec extends AnyFlatSpec with Matchers {

  private val schemaPath =
    getClass.getResource("/schemas/simple.asn1").getPath

  // SimpleRecord ::= SEQUENCE { id INTEGER, name UTF8String, active BOOLEAN,
  //                             score INTEGER(0..100), data OCTET STRING, oid OID }
  private def makeSimpleRecord(id: Int, name: String): Array[Byte] =
    new DERSequence(Array[ASN1Encodable](
      new ASN1Integer(id),
      new DERUTF8String(name),
      ASN1Boolean.getInstance(true),
      new ASN1Integer(42),
      new DEROctetString(Array[Byte](0xde.toByte, 0xad.toByte, 0xbe.toByte, 0xef.toByte)),
      new ASN1ObjectIdentifier("1.2.3.4.5")
    )).getEncoded("DER")

  private def writeTempBer(records: Seq[Array[Byte]]): File = {
    val f   = Files.createTempFile("inspector-test", ".ber").toFile
    val fos = new FileOutputStream(f)
    try records.foreach(fos.write) finally fos.close()
    f
  }

  "Asn1Inspector.peek" should "return a report containing decoded field values" in {
    val f      = writeTempBer(Seq(makeSimpleRecord(1, "Alice"), makeSimpleRecord(2, "Bob")))
    val report = Asn1Inspector.peek(
      schemaPaths = Seq(schemaPath),
      typeName    = "SimpleRecord",
      encoding    = "ber",
      filePath    = f.getAbsolutePath,
      maxRecords  = 2
    )

    report should include ("Record 1")
    report should include ("Record 2")
    report should include ("Alice")
    report should include ("Bob")
    report should include ("id")
    report should include ("name")
    report should include ("active")
    report should include ("1.2.3.4.5")
    report should include ("decoded 2 record(s)")
  }

  it should "limit output to maxRecords" in {
    val f      = writeTempBer(Seq(
      makeSimpleRecord(1, "One"),
      makeSimpleRecord(2, "Two"),
      makeSimpleRecord(3, "Three")
    ))
    val report = Asn1Inspector.peek(
      schemaPaths = Seq(schemaPath),
      typeName    = "SimpleRecord",
      encoding    = "ber",
      filePath    = f.getAbsolutePath,
      maxRecords  = 2
    )

    report should include ("decoded 2 record(s)")
    report should not include "Three"
  }

  it should "report 0 records when the type name is wrong" in {
    val f      = writeTempBer(Seq(makeSimpleRecord(1, "X")))
    val report = Asn1Inspector.peek(
      schemaPaths = Seq(schemaPath),
      typeName    = "NoSuchType",
      encoding    = "ber",
      filePath    = f.getAbsolutePath
    )

    report should include ("ERROR")
    report should include ("NoSuchType")
  }

  it should "report 0 records and list available types when file has wrong encoding" in {
    val f      = writeTempBer(Seq(makeSimpleRecord(1, "X")))
    val report = Asn1Inspector.peek(
      schemaPaths = Seq(schemaPath),
      typeName    = "SimpleRecord",
      encoding    = "xer",          // wrong encoding for a BER file
      filePath    = f.getAbsolutePath,
      maxRecords  = 1
    )

    report should include ("0 records")
    report should include ("SimpleRecord")   // listed under available types
  }

  it should "handle an empty file gracefully" in {
    val f      = writeTempBer(Seq.empty)
    val report = Asn1Inspector.peek(
      schemaPaths = Seq(schemaPath),
      typeName    = "SimpleRecord",
      encoding    = "ber",
      filePath    = f.getAbsolutePath
    )

    report should include ("0 records")
  }
}
