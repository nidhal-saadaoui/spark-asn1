package io.github.sparkasn1.spark.asn1.parser

import io.github.sparkasn1.spark.asn1.model.Asn1Module
import io.github.sparkasn1.spark.asn1.parser.antlr.{ASNLexer, ASNParser}
import org.antlr.v4.runtime.{CharStreams, CommonTokenStream}

import java.io.InputStream
import java.nio.charset.StandardCharsets

object Asn1SchemaParser {

  /** Parse a .asn1 file, returning all modules defined in it (usually 1). */
  def parseFiles(path: String): Seq[Asn1Module] = {
    val stream = CharStreams.fromFileName(path)
    parseCharStream(stream)
  }

  /** Convenience: parse a file and return the first (and usually only) module. */
  def parseFile(path: String): Asn1Module = {
    parseFiles(path).headOption.getOrElse(
      throw new Asn1SchemaException(s"No modules found in '$path'"))
  }

  /** Parse all modules from an InputStream. */
  def parseStream(is: InputStream, sourceName: String = "<stream>"): Seq[Asn1Module] = {
    val stream = CharStreams.fromStream(is, StandardCharsets.UTF_8)
    parseCharStream(stream)
  }

  /** Parse all modules from a String (useful in tests). */
  def parseString(asn1: String, sourceName: String = "<string>"): Asn1Module = {
    val stream = CharStreams.fromString(asn1, sourceName)
    parseCharStream(stream).headOption.getOrElse(
      throw new Asn1SchemaException(s"No modules found in schema string"))
  }

  private def parseCharStream(stream: org.antlr.v4.runtime.CharStream): Seq[Asn1Module] = {
    val lexer   = new ASNLexer(stream)
    val tokens  = new CommonTokenStream(lexer)
    val parser  = new ASNParser(tokens)
    val tree    = parser.modules()
    val builder = new Asn1AstBuilder()
    builder.visit(tree).asInstanceOf[Seq[Asn1Module]]
  }
}
