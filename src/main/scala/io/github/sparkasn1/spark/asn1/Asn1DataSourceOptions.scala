package io.github.sparkasn1.spark.asn1

/** Keys and defaults for all spark-asn1 reader / writer options. */
object Asn1DataSourceOptions {

  // Required
  val SCHEMA_PATH  = "asn1.schema"  // comma-separated paths to .asn1 files
  val ROOT_TYPE    = "asn1.type"    // ASN.1 type name to decode as the root record

  // Encoding selection
  val ENCODING     = "asn1.encoding" // ber | der | per-aligned | per-unaligned | xer
  val DEFAULT_ENCODING = "ber"

  // PER framing (Phase 2)
  val PER_FRAMING      = "asn1.per.framing"       // length-prefixed | fixed-length | hex-lines
  val PER_RECORD_BYTES = "asn1.per.record.bytes"  // for fixed-length framing
  val DEFAULT_PER_FRAMING = "length-prefixed"

  // Type display options
  val ENUMERATED_AS_INT  = "asn1.enumerated.as.int"   // true | false
  val CHOICE_TAG_FIELD   = "asn1.choice.tag.field"    // default "_tag"
  val BITSTRING_LEN_FIELD = "asn1.bitstring.length.field" // true | false

  val DEFAULT_CHOICE_TAG_FIELD = "_tag"

  /** Parse and validate options, throwing IllegalArgumentException for missing required keys. */
  case class ParsedOptions(
    schemaPaths: Seq[String],
    rootType: String,
    encoding: Encoding,
    perFraming: PerFraming,
    perRecordBytes: Option[Int],
    enumeratedAsInt: Boolean,
    choiceTagField: String
  )

  sealed trait Encoding
  object Encoding {
    case object Ber          extends Encoding
    case object Der          extends Encoding
    case object PerAligned   extends Encoding
    case object PerUnaligned extends Encoding
    case object Xer          extends Encoding
  }

  sealed trait PerFraming
  object PerFraming {
    case object LengthPrefixed extends PerFraming
    case object FixedLength    extends PerFraming
    case object HexLines       extends PerFraming
  }

  def parse(options: Map[String, String]): ParsedOptions = {
    val schemaPaths = options.getOrElse(SCHEMA_PATH,
      throw new IllegalArgumentException(s"Required option '$SCHEMA_PATH' is missing"))
      .split(",").map(_.trim).filter(_.nonEmpty).toSeq

    val rootType = options.getOrElse(ROOT_TYPE,
      throw new IllegalArgumentException(s"Required option '$ROOT_TYPE' is missing"))

    val encoding: Encoding = options.getOrElse(ENCODING, DEFAULT_ENCODING).toLowerCase match {
      case "ber"          => Encoding.Ber
      case "der"          => Encoding.Der
      case "per-aligned"  => Encoding.PerAligned
      case "per-unaligned" => Encoding.PerUnaligned
      case "xer"          => Encoding.Xer
      case other =>
        throw new IllegalArgumentException(s"Unknown encoding '$other'. Valid values: ber, der, per-aligned, per-unaligned, xer")
    }

    val perFraming: PerFraming = options.getOrElse(PER_FRAMING, DEFAULT_PER_FRAMING).toLowerCase match {
      case "length-prefixed" => PerFraming.LengthPrefixed
      case "fixed-length"    => PerFraming.FixedLength
      case "hex-lines"       => PerFraming.HexLines
      case other =>
        throw new IllegalArgumentException(s"Unknown PER framing '$other'. Valid: length-prefixed, fixed-length, hex-lines")
    }

    val perRecordBytes: Option[Int] = options.get(PER_RECORD_BYTES).map { s =>
      scala.util.Try(s.toInt).getOrElse(throw new IllegalArgumentException(s"'$PER_RECORD_BYTES' must be an integer"))
    }
    if (perFraming == PerFraming.FixedLength && perRecordBytes.isEmpty)
      throw new IllegalArgumentException(s"'$PER_RECORD_BYTES' is required when '$PER_FRAMING=fixed-length'")

    ParsedOptions(
      schemaPaths    = schemaPaths,
      rootType       = rootType,
      encoding       = encoding,
      perFraming     = perFraming,
      perRecordBytes = perRecordBytes,
      enumeratedAsInt = options.get(ENUMERATED_AS_INT).exists(_.toLowerCase == "true"),
      choiceTagField = options.getOrElse(CHOICE_TAG_FIELD, DEFAULT_CHOICE_TAG_FIELD)
    )
  }
}
