package io.github.sparkasn1.spark.asn1.codec.per

import io.github.sparkasn1.spark.asn1.parser.SchemaRegistry

class UnalignedPerEncoder(
  registry:       SchemaRegistry,
  moduleName:     String,
  choiceTagField: String = "_tag"
) extends PerEncoder(registry, moduleName, aligned = false, choiceTagField = choiceTagField)
