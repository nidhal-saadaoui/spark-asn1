package io.github.sparkasn1.spark.asn1.util

import io.github.sparkasn1.spark.asn1.parser.{Asn1SchemaParser, SchemaRegistry}

import java.io.File
import java.util.concurrent.ConcurrentHashMap

/**
 * Executor-local cache of parsed SchemaRegistry instances.
 *
 * Keyed on the set of (path, lastModified) pairs so that schema file changes
 * are detected between jobs without restarting executors.
 *
 * Schema files must be accessible from every executor node — either on HDFS,
 * S3, or distributed via --files / SparkContext.addFile.
 */
object SchemaCache {

  private type CacheKey = Set[(String, Long)]
  private val cache = new ConcurrentHashMap[CacheKey, SchemaRegistry]()

  def getOrParse(paths: Seq[String]): SchemaRegistry = {
    val key = cacheKey(paths)
    // computeIfAbsent gives us double-checked locking for free
    cache.computeIfAbsent(key, _ => parse(paths))
  }

  def invalidate(): Unit = cache.clear()

  private def parse(paths: Seq[String]): SchemaRegistry = {
    val modules = paths.flatMap(Asn1SchemaParser.parseFiles)
    SchemaRegistry(modules)
  }

  private def cacheKey(paths: Seq[String]): CacheKey =
    paths.map { p =>
      val f = new File(p)
      (p, if (f.exists()) f.lastModified() else -1L)
    }.toSet
}
