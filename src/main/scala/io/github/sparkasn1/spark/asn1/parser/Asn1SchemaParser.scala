package io.github.sparkasn1.spark.asn1.parser

import io.github.sparkasn1.spark.asn1.model.Asn1Module
import io.github.sparkasn1.spark.asn1.parser.antlr.{ASNLexer, ASNParser}
import org.antlr.v4.runtime._

import java.io.InputStream
import java.nio.charset.StandardCharsets

object Asn1SchemaParser {

  /** Parse a .asn1 file, returning all modules defined in it (usually 1). */
  def parseFiles(path: String): Seq[Asn1Module] = {
    val stream = CharStreams.fromFileName(path)
    parseCharStream(stream, path)
  }

  /** Convenience: parse a file and return the first (and usually only) module. */
  def parseFile(path: String): Asn1Module =
    parseFiles(path).headOption.getOrElse(
      throw new Asn1SchemaException(s"No modules found in '$path'"))

  /** Parse all modules from an InputStream. */
  def parseStream(is: InputStream, sourceName: String = "<stream>"): Seq[Asn1Module] = {
    val stream = CharStreams.fromStream(is, StandardCharsets.UTF_8)
    parseCharStream(stream, sourceName)
  }

  /** Parse all modules from a String (useful in tests). */
  def parseString(asn1: String, sourceName: String = "<string>"): Asn1Module = {
    val stream = CharStreams.fromString(asn1, sourceName)
    parseCharStream(stream, sourceName).headOption.getOrElse(
      throw new Asn1SchemaException(s"No modules found in '$sourceName'"))
  }

  private def parseCharStream(stream: CharStream, sourceName: String): Seq[Asn1Module] = {
    val errors  = new CollectingErrorListener(sourceName)
    val lexer   = new ASNLexer(stream)
    lexer.removeErrorListeners()
    lexer.addErrorListener(errors)
    val tokens  = new CommonTokenStream(lexer)
    val parser  = new ASNParser(tokens)
    parser.removeErrorListeners()
    parser.addErrorListener(errors)
    val tree    = parser.modules()
    errors.throwIfAny()
    try new Asn1AstBuilder().visit(tree).asInstanceOf[Seq[Asn1Module]]
    catch {
      case e: Asn1SchemaException => throw e
      case e: Exception =>
        throw new Asn1SchemaException(s"AST build failed in '$sourceName': ${e.getMessage}", e)
    }
  }
}

/** ANTLR error listener that collects all syntax errors and throws one combined exception. */
private final class CollectingErrorListener(sourceName: String)
    extends BaseErrorListener {

  private val errors = new java.util.ArrayList[String]()

  override def syntaxError(
    recognizer:         Recognizer[_, _],
    offendingSymbol:    AnyRef,
    line:               Int,
    charPositionInLine: Int,
    msg:                String,
    e:                  RecognitionException
  ): Unit = {
    val sym = Option(offendingSymbol).fold("")(s => s" (found: $s)")
    errors.add(s"$sourceName:$line:$charPositionInLine — $msg$sym")
  }

  def throwIfAny(): Unit =
    if (!errors.isEmpty) {
      val detail = {
        import scala.jdk.CollectionConverters._
        errors.asScala.mkString("\n  ")
      }
      throw new Asn1SchemaException(
        s"${errors.size()} syntax error(s) in '$sourceName':\n  $detail")
    }
}
