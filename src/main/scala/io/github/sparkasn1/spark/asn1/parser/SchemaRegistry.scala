package io.github.sparkasn1.spark.asn1.parser

import io.github.sparkasn1.spark.asn1.model._

/**
 * Resolves type references across one or more parsed Asn1Modules.
 * After construction all Asn1TypeReference nodes in every module's
 * type assignments are resolved to their concrete types.
 */
class SchemaRegistry(rawModules: Seq[Asn1Module]) {

  /** Module name → fully resolved Asn1Module */
  val modules: Map[String, Asn1Module] = {
    val moduleMap = rawModules.map(m => m.name -> m).toMap
    // Topological sort by import dependencies, then resolve references.
    val ordered = topologicalSort(rawModules, moduleMap)
    val resolved = scala.collection.mutable.Map.empty[String, Asn1Module]
    ordered.foreach { m =>
      val resolvedTypes = m.typeAssignments.map { case (n, ta) =>
        n -> ta.copy(asn1Type = resolveType(ta.asn1Type, m.name, moduleMap, resolved.toMap))
      }
      resolved(m.name) = m.copy(typeAssignments = resolvedTypes)
    }
    resolved.toMap
  }

  /** Look up a fully-qualified type (module.typeName or just typeName using currentModule). */
  def resolve(typeName: String, currentModule: String): Option[Asn1Type] =
    modules.get(currentModule)
      .flatMap(_.typeAssignments.get(typeName))
      .map(_.asn1Type)
      .orElse {
        modules.values.flatMap(_.typeAssignments.get(typeName)).headOption.map(_.asn1Type)
      }

  def resolveRef(ref: Asn1TypeReference, currentModule: String): Asn1Type =
    ref.moduleName match {
      case Some(mod) =>
        modules.get(mod)
          .flatMap(_.typeAssignments.get(ref.typeName))
          .map(_.asn1Type)
          .getOrElse(ref)
      case None =>
        resolve(ref.typeName, currentModule).getOrElse(ref)
    }

  /** Return the tag default of a module. */
  def tagDefaultOf(moduleName: String): TagDefault =
    modules.get(moduleName).map(_.tagDefault).getOrElse(TagDefault.None)

  // -----------------------------------------------------------------------
  // Private helpers
  // -----------------------------------------------------------------------

  private def topologicalSort(
    modules: Seq[Asn1Module],
    byName: Map[String, Asn1Module]
  ): Seq[Asn1Module] = {
    val visited = scala.collection.mutable.Set.empty[String]
    val result  = scala.collection.mutable.ArrayBuffer.empty[Asn1Module]

    def visit(m: Asn1Module, stack: Set[String]): Unit = {
      if (stack.contains(m.name))
        throw new Asn1SchemaException(s"Circular module import detected: ${stack.mkString(" -> ")} -> ${m.name}")
      if (!visited.contains(m.name)) {
        m.imports.foreach { sfm =>
          byName.get(sfm.fromModule).foreach(dep => visit(dep, stack + m.name))
        }
        visited += m.name
        result += m
      }
    }

    modules.foreach(m => visit(m, Set.empty))
    result.toSeq
  }

  private def resolveType(
    t: Asn1Type,
    currentModule: String,
    rawByName: Map[String, Asn1Module],
    resolvedSoFar: Map[String, Asn1Module]
  ): Asn1Type = {
    def rec(t: Asn1Type): Asn1Type = t match {
      case ref: Asn1TypeReference =>
        val resolved = ref.moduleName match {
          case Some(mod) =>
            resolvedSoFar.get(mod)
              .flatMap(_.typeAssignments.get(ref.typeName))
              .map(_.asn1Type)
          case None =>
            resolvedSoFar.get(currentModule)
              .flatMap(_.typeAssignments.get(ref.typeName))
              .orElse(resolvedSoFar.values.flatMap(_.typeAssignments.get(ref.typeName)).headOption)
              .map(_.asn1Type)
        }
        resolved.getOrElse(ref)

      case s: Asn1Sequence  => s.copy(components = s.components.map(c => c.copy(asn1Type = rec(c.asn1Type))))
      case s: Asn1Set       => s.copy(components = s.components.map(c => c.copy(asn1Type = rec(c.asn1Type))))
      case s: Asn1SequenceOf => s.copy(elementType = rec(s.elementType))
      case s: Asn1SetOf      => s.copy(elementType = rec(s.elementType))
      case c: Asn1Choice     => c.copy(alternatives = c.alternatives.map(a => a.copy(asn1Type = rec(a.asn1Type))))
      case tt: Asn1TaggedType => tt.copy(innerType = rec(tt.innerType))
      case other             => other
    }
    rec(t)
  }
}

object SchemaRegistry {
  def apply(modules: Seq[Asn1Module]): SchemaRegistry = new SchemaRegistry(modules)
  def apply(module: Asn1Module): SchemaRegistry       = new SchemaRegistry(Seq(module))
}

class Asn1SchemaException(msg: String, cause: Throwable = null)
  extends RuntimeException(msg, cause)
