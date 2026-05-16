package io.github.sparkasn1.spark.asn1.codec.per

import io.github.sparkasn1.spark.asn1.parser.SchemaRegistry

/** Aligned PER (APER) decoder. Multi-byte quantities are byte-aligned before reading. */
class AlignedPerDecoder(registry: SchemaRegistry, moduleName: String)
  extends PerDecoder(registry, moduleName, aligned = true)
