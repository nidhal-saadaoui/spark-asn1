package io.github.sparkasn1.spark.asn1.model

/** Inclusive size constraint. Long.MaxValue means unbounded. */
case class SizeConstraint(min: Long, max: Long)

/** Inclusive value range constraint for INTEGER. Long.MinValue / Long.MaxValue mean MIN/MAX. */
case class ValueRangeConstraint(min: Long, max: Long)
