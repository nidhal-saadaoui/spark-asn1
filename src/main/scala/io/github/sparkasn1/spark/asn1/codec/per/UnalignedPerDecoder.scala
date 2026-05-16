package io.github.sparkasn1.spark.asn1.codec.per

import io.github.sparkasn1.spark.asn1.parser.SchemaRegistry

/** Unaligned PER (UPER) decoder. All values are packed without byte alignment. */
class UnalignedPerDecoder(registry: SchemaRegistry, moduleName: String)
  extends PerDecoder(registry, moduleName, aligned = false)
