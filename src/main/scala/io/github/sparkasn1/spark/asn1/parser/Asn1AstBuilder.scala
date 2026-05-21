package io.github.sparkasn1.spark.asn1.parser

import io.github.sparkasn1.spark.asn1.model._
import io.github.sparkasn1.spark.asn1.parser.antlr.ASNParser._
import io.github.sparkasn1.spark.asn1.parser.antlr.{ASNBaseVisitor, ASNParser}
import org.antlr.v4.runtime.tree.ParseTree

import scala.jdk.CollectionConverters._

/** Translates an ANTLR4 parse tree into the internal Asn1Module ADT. */
class Asn1AstBuilder extends ASNBaseVisitor[AnyRef] {

  // Per-module integer constants (value assignments), populated before type visiting.
  private var moduleValueConstants: Map[String, Long] = Map.empty

  override def visitModules(ctx: ModulesContext): AnyRef =
    ctx.moduleDefinition().asScala.map(visitModuleDefinition(_).asInstanceOf[Asn1Module]).toSeq

  override def visitModuleDefinition(ctx: ModuleDefinitionContext): AnyRef = {
    val name = ctx.IDENTIFIER(0).getText

    val tagDefault: TagDefault = {
      val td = ctx.tagDefault()
      if (td == null || td.getChildCount() == 0) TagDefault.None
      else if (td.EXPLICIT_LITERAL()  != null) TagDefault.Explicit
      else if (td.IMPLICIT_LITERAL()  != null) TagDefault.Implicit
      else if (td.AUTOMATIC_LITERAL() != null) TagDefault.Automatic
      else TagDefault.None
    }

    val extensibilityImplied = {
      val ed = ctx.extensionDefault()
      ed != null && ed.EXTENSIBILITY_LITERAL() != null
    }

    val body = ctx.moduleBody()
    val imports: Seq[SymbolsFromModule] =
      if (body == null || body.imports() == null) Seq.empty
      else visitImports(body.imports())

    // Pre-pass: collect integer value constants so constraints can reference them by name.
    moduleValueConstants =
      if (body == null || body.assignmentList() == null) Map.empty
      else body.assignmentList().assignment().asScala.flatMap { a =>
        if (a.valueAssignment() != null) {
          val raw = if (a.valueAssignment().value() != null) a.valueAssignment().value().getText else ""
          scala.util.Try(raw.toLong).toOption.map(a.IDENTIFIER().getText -> _)
        } else None
      }.toMap

    val (typeAssignments, valueAssignments): (Map[String, TypeAssignment], Map[String, ValueAssignment]) =
      if (body == null || body.assignmentList() == null) (Map.empty, Map.empty)
      else {
        val allAssignments = body.assignmentList().assignment().asScala.flatMap { a =>
          Option(visitAssignment(a)).map {
            case ta: TypeAssignment  => Left(ta)
            case va: ValueAssignment => Right(va)
            case _                   => null
          }.filter(_ != null)
        }
        val types  = allAssignments.collect { case Left(ta) => ta.name -> ta }.toMap
        val values = allAssignments.collect { case Right(va) => va.name -> va }.toMap
        (types, values)
      }

    Asn1Module(name, tagDefault, extensibilityImplied, imports, typeAssignments, valueAssignments)
  }

  override def visitImports(ctx: ImportsContext): Seq[SymbolsFromModule] = {
    if (ctx == null || ctx.symbolsImported() == null) return Seq.empty
    val sfl = ctx.symbolsImported().symbolsFromModuleList()
    if (sfl == null) return Seq.empty
    sfl.symbolsFromModule().asScala.map { sfm =>
      val symbols = sfm.symbolList().symbol().asScala.map(_.IDENTIFIER().getText).toSeq
      val fromModule = sfm.globalModuleReference().IDENTIFIER().getText
      SymbolsFromModule(symbols, fromModule)
    }.toSeq
  }

  override def visitAssignment(ctx: AssignmentContext): AnyRef = {
    val name = ctx.IDENTIFIER().getText
    if (ctx.typeAssignment() != null) {
      val t = visitAsnType(ctx.typeAssignment().asnType())
      TypeAssignment(name, t)
    } else if (ctx.valueAssignment() != null) {
      val va = ctx.valueAssignment()
      val t  = visitAsnType(va.asnType())
      val raw = if (va.value() != null) va.value().getText else ""
      ValueAssignment(name, t, raw)
    } else {
      null // parameterizedAssignment / objectClassAssignment — skip
    }
  }

  // -------------------------------------------------------------------------
  // Type visitors
  // -------------------------------------------------------------------------

  override def visitAsnType(ctx: AsnTypeContext): Asn1Type = {
    val base: Asn1Type =
      if (ctx.taggedType() != null) visitTaggedType(ctx.taggedType())
      else if (ctx.builtinType() != null) visitBuiltinType(ctx.builtinType())
      else visitReferencedType(ctx.referencedType())

    // Apply constraints (SIZE only — value range is on the inner INTEGER)
    ctx.constraint().asScala.foldLeft(base) { (t, c) => applyConstraint(t, c) }
  }

  override def visitTaggedType(ctx: TaggedTypeContext): Asn1Type = {
    val tagCtx    = ctx.tag()
    val tagNum    = tagCtx.NUMBER().getText.toInt
    val tagClass: TagClass = {
      val tc = tagCtx.tagClass()
      if (tc.UNIVERSAL_LITERAL()   != null) TagClass.Universal
      else if (tc.APPLICATION_LITERAL() != null) TagClass.Application
      else if (tc.PRIVATE_LITERAL()     != null) TagClass.Private
      else TagClass.ContextSpecific
    }
    val tagging: Tagging = {
      val tt = ctx.tagType()
      if (tt == null)                       Tagging.Automatic
      else if (tt.IMPLICIT_LITERAL() != null) Tagging.Implicit
      else                                    Tagging.Explicit
    }
    val inner = visitAsnType(ctx.asnType())
    Asn1TaggedType(tagClass, tagNum, tagging, inner)
  }

  override def visitBuiltinType(ctx: BuiltinTypeContext): Asn1Type = {
    if (ctx.octetStringType()  != null) Asn1OctetString
    else if (ctx.bitStringType()   != null) visitBitStringType(ctx.bitStringType())
    else if (ctx.choiceType()      != null) visitChoiceType(ctx.choiceType())
    else if (ctx.enumeratedType()  != null) visitEnumeratedType(ctx.enumeratedType())
    else if (ctx.integerType()     != null) visitIntegerType(ctx.integerType())
    else if (ctx.sequenceType()    != null) visitSequenceType(ctx.sequenceType())
    else if (ctx.sequenceOfType()  != null) visitSequenceOfType(ctx.sequenceOfType())
    else if (ctx.setType()         != null) visitSetType(ctx.setType())
    else if (ctx.setOfType()       != null) visitSetOfType(ctx.setOfType())
    else if (ctx.objectidentifiertype() != null) Asn1ObjectIdentifier
    else if (ctx.restrictedCharacterStringType() != null)
      visitRestrictedCharacterStringType(ctx.restrictedCharacterStringType())
    else if (ctx.BOOLEAN_LITERAL() != null) Asn1Boolean
    else if (ctx.NULL_LITERAL()    != null) Asn1Null
    else if (ctx.REAL_LITERAL()    != null) Asn1Real
    else Asn1Any
  }

  override def visitRestrictedCharacterStringType(ctx: RestrictedCharacterStringTypeContext): Asn1StringType = {
    if      (ctx.UTF8STRING_LITERAL()        != null) Asn1Utf8String
    else if (ctx.NUMERICSTRING_LITERAL()     != null) Asn1NumericString
    else if (ctx.PRINTABLESTRING_LITERAL()   != null) Asn1PrintableString
    else if (ctx.TELETEXSTRING_LITERAL()     != null) Asn1TeletexString
    else if (ctx.VIDEOTEXSTRING_LITERAL()    != null) Asn1VideotexString
    else if (ctx.IA5STRING_LITERAL()         != null) Asn1Ia5String
    else if (ctx.GRAPHICSTRING_LITERAL()     != null) Asn1GraphicString
    else if (ctx.VISIBLESTRING_LITERAL()     != null) Asn1VisibleString
    else if (ctx.GENERALSTRING_LITERAL()     != null) Asn1GeneralString
    else if (ctx.UNIVERSALSTRING_LITERAL()   != null) Asn1UniversalString
    else if (ctx.BMPSTRING_LITERAL()         != null) Asn1BmpString
    else if (ctx.GENERALIZEDTIME_LITERAL()   != null) Asn1GeneralizedTime
    else                                              Asn1UtcTime
  }

  override def visitIntegerType(ctx: IntegerTypeContext): Asn1Type = Asn1Integer()

  override def visitBitStringType(ctx: BitStringTypeContext): Asn1Type = {
    val namedBits: Seq[NamedBit] =
      if (ctx.namedBitList() == null) Seq.empty
      else ctx.namedBitList().namedBit().asScala.map { nb =>
        NamedBit(nb.IDENTIFIER().getText, nb.NUMBER().getText.toInt)
      }.toSeq
    Asn1BitString(namedBits)
  }

  override def visitSequenceType(ctx: SequenceTypeContext): Asn1Type = {
    val (components, extensible) = parseComponentTypeLists(ctx.componentTypeLists(), ctx.extensionAndException())
    Asn1Sequence(components, extensible)
  }

  override def visitSequenceOfType(ctx: SequenceOfTypeContext): Asn1Type = {
    val elemType =
      if (ctx.asnType() != null) visitAsnType(ctx.asnType())
      else visitAsnType(ctx.namedType().asnType())
    val size = extractSizeFromSequenceOf(ctx)
    Asn1SequenceOf(elemType, size)
  }

  private def extractSizeFromSequenceOf(ctx: SequenceOfTypeContext): Option[SizeConstraint] = {
    if (ctx.sizeConstraint() != null) extractSizeFromSizeConstraint(ctx.sizeConstraint())
    else if (ctx.constraint() != null) extractSizeFromConstraint(ctx.constraint())
    else None
  }

  override def visitSetType(ctx: SetTypeContext): Asn1Type = {
    val (components, extensible) = parseComponentTypeLists(ctx.componentTypeLists(), ctx.extensionAndException())
    Asn1Set(components, extensible)
  }

  override def visitSetOfType(ctx: SetOfTypeContext): Asn1Type = {
    val elemType =
      if (ctx.asnType() != null) visitAsnType(ctx.asnType())
      else visitAsnType(ctx.namedType().asnType())
    val size: Option[SizeConstraint] =
      if (ctx.sizeConstraint() != null) extractSizeFromSizeConstraint(ctx.sizeConstraint())
      else if (ctx.constraint() != null) extractSizeFromConstraint(ctx.constraint())
      else None
    Asn1SetOf(elemType, size)
  }

  override def visitChoiceType(ctx: ChoiceTypeContext): Asn1Type = {
    val altLists = ctx.alternativeTypeLists()
    val extensible = altLists.extensionAndException() != null
    val rootAlts = altLists.rootAlternativeTypeList()
      .alternativeTypeList().namedType().asScala.map { nt =>
        NamedType(nt.IDENTIFIER().getText, visitAsnType(nt.asnType()))
      }.toSeq
    Asn1Choice(rootAlts, extensible)
  }

  override def visitEnumeratedType(ctx: EnumeratedTypeContext): Asn1Type = {
    val items = ctx.enumerations().rootEnumeration().enumeration()
      .enumerationItem().asScala.zipWithIndex.map { case (item, idx) =>
        val name = if (item.IDENTIFIER() != null) item.IDENTIFIER().getText
                   else if (item.namedNumber() != null) item.namedNumber().IDENTIFIER().getText
                   else item.getText
        val value: Option[Long] =
          if (item.namedNumber() != null) Some(item.namedNumber().signedNumber().getText.toLong)
          else None
        EnumerationItem(name, value.orElse(Some(idx.toLong)))
      }.toSeq
    Asn1Enumerated(items)
  }

  override def visitReferencedType(ctx: ReferencedTypeContext): Asn1Type = {
    val dt = ctx.definedType()
    val parts = dt.IDENTIFIER().asScala.map(_.getText)
    if (parts.size == 2) Asn1TypeReference(Some(parts(0)), parts(1))
    else                 Asn1TypeReference(None, parts(0))
  }

  // -------------------------------------------------------------------------
  // Component type list helpers
  // -------------------------------------------------------------------------

  private def parseComponentTypeLists(
    ctx: ComponentTypeListsContext,
    extCtx: ExtensionAndExceptionContext
  ): (Seq[ComponentType], Boolean) = {
    if (ctx == null && extCtx == null) return (Seq.empty, false)
    val hasExt = extCtx != null ||
      (ctx != null && (ctx.extensionAndException() != null || ctx.extensionAdditions() != null))
    val rootComponents: Seq[ComponentType] =
      if (ctx == null) Seq.empty
      else ctx.rootComponentTypeList().asScala
            .flatMap(_.componentTypeList().componentType().asScala)
            .flatMap(parseComponentType).toSeq
    val extensionComponents: Seq[ComponentType] =
      if (ctx == null || ctx.extensionAdditions() == null) Seq.empty
      else {
        val eal = ctx.extensionAdditions().extensionAdditionList()
        if (eal == null) Seq.empty
        else eal.extensionAddition().asScala.flatMap { ea =>
          if (ea.componentType() != null)
            parseComponentType(ea.componentType()).map(c => c.copy(optional = true)).toSeq
          else if (ea.extensionAdditionGroup() != null) {
            val eag = ea.extensionAdditionGroup()
            if (eag.componentTypeList() == null) Seq.empty
            else eag.componentTypeList().componentType().asScala
                  .flatMap(parseComponentType)
                  .map(c => c.copy(optional = true)).toSeq
          } else Seq.empty
        }.toSeq
      }
    (rootComponents ++ extensionComponents, hasExt)
  }

  private def parseComponentType(ctx: ComponentTypeContext): Option[ComponentType] = {
    if (ctx.namedType() == null) return None // COMPONENTS OF — skip
    val nt      = ctx.namedType()
    val name    = nt.IDENTIFIER().getText
    val t       = visitAsnType(nt.asnType())
    val optional = ctx.OPTIONAL_LITERAL() != null || ctx.DEFAULT_LITERAL() != null
    val default  = if (ctx.DEFAULT_LITERAL() != null && ctx.value() != null)
                     Some(ctx.value().getText) else None
    Some(ComponentType(name, t, optional, default))
  }

  // -------------------------------------------------------------------------
  // Constraint extraction helpers
  // -------------------------------------------------------------------------

  private def applyConstraint(t: Asn1Type, ctx: ConstraintContext): Asn1Type = {
    val cs = ctx.constraintSpec()
    if (cs.subtypeConstraint() != null) {
      val stc = cs.subtypeConstraint()
      // Look for SIZE constraint
      val size = findSizeConstraint(stc)
      // Look for value range (applies to INTEGER)
      val range = findValueRange(stc)
      t match {
        case i: Asn1Integer if range.isDefined       => i.copy(constraint = range)
        case b: Asn1BitString if size.isDefined      => b.copy(sizeConstraint = size)
        case s: Asn1SequenceOf if size.isDefined     => s.copy(sizeConstraint = size)
        case s: Asn1SetOf if size.isDefined          => s.copy(sizeConstraint = size)
        case _                                       => t
      }
    } else t
  }

  private def findSizeConstraint(ctx: SubtypeConstraintContext): Option[SizeConstraint] = {
    val elements = ctx.elementSetSpecs()
    findSizeInElementSetSpecs(elements)
  }

  // rootElementSetSpec() returns a SINGLE context (appears once in elementSetSpecs rule).
  // unions() returns a SINGLE context (appears once in elementSetSpec rule).
  // intersections() returns a LIST (repeats via (unionMark intersections)*).
  // intersectionElements() returns a LIST (repeats via (intersectionMark intersectionElements)*).

  private def findSizeInElementSetSpecs(ctx: ElementSetSpecsContext): Option[SizeConstraint] =
    Option(ctx)
      .flatMap(c => Option(c.rootElementSetSpec()))
      .flatMap(res => findSizeInElementSetSpec(res.elementSetSpec()))

  private def findSizeInElementSetSpec(ctx: ElementSetSpecContext): Option[SizeConstraint] =
    Option(ctx)
      .flatMap(c => Option(c.unions()))
      .flatMap { u =>
        u.intersections().asScala.flatMap { i =>
          i.intersectionElements().asScala.flatMap { ie =>
            Option(ie.elements()).flatMap { el =>
              val ste = el.subtypeElements()
              if (ste != null && ste.sizeConstraint() != null)
                extractSizeFromSizeConstraint(ste.sizeConstraint())
              else None
            }
          }
        }.headOption
      }

  private def findValueRange(ctx: SubtypeConstraintContext): Option[ValueRangeConstraint] =
    Option(ctx.elementSetSpecs())
      .flatMap(ess => Option(ess.rootElementSetSpec()))
      .flatMap(res => findValueRangeInElementSetSpec(res.elementSetSpec()))

  private def findValueRangeInElementSetSpec(ctx: ElementSetSpecContext): Option[ValueRangeConstraint] =
    Option(ctx)
      .flatMap(c => Option(c.unions()))
      .flatMap { u =>
        u.intersections().asScala.flatMap { i =>
          i.intersectionElements().asScala.flatMap { ie =>
            Option(ie.elements()).flatMap { el =>
              val ste = el.subtypeElements()
              if (ste != null && ste.DOUBLE_DOT() != null && ste.sizeConstraint() == null) {
                val lo = parseEndpoint(ste, isFirst = true)
                val hi = parseEndpoint(ste, isFirst = false)
                Some(ValueRangeConstraint(lo, hi))
              } else None
            }
          }
        }.headOption
      }

  private def resolveEndpointText(text: String, fallback: Long): Long =
    scala.util.Try(text.toLong).toOption
      .orElse(moduleValueConstants.get(text))
      .getOrElse(fallback)

  private def parseEndpoint(ctx: SubtypeElementsContext, isFirst: Boolean): Long = {
    val values = ctx.value().asScala
    if (isFirst) {
      if (ctx.MIN_LITERAL() != null) Long.MinValue
      else values.headOption.map(v => resolveEndpointText(v.getText, Long.MinValue))
                            .getOrElse(Long.MinValue)
    } else {
      if (ctx.MAX_LITERAL() != null) Long.MaxValue
      else values.lastOption.map(v => resolveEndpointText(v.getText, Long.MaxValue))
                            .getOrElse(Long.MaxValue)
    }
  }

  private def extractSizeFromSizeConstraint(ctx: SizeConstraintContext): Option[SizeConstraint] = {
    val cs = Option(ctx.constraint())
      .flatMap(c => Option(c.constraintSpec()))
      .flatMap(c => Option(c.subtypeConstraint()))
    cs.flatMap { stc =>
      Option(stc.elementSetSpecs())
        .flatMap(ess => Option(ess.rootElementSetSpec()))
        .flatMap(res => Option(res.elementSetSpec()))
        .flatMap(es  => Option(es.unions()))
        .flatMap { u =>
          u.intersections().asScala.flatMap { i =>
            i.intersectionElements().asScala.flatMap { ie =>
              Option(ie.elements()).flatMap { el =>
                val ste = el.subtypeElements()
                if (ste == null) None
                else if (ste.DOUBLE_DOT() != null) {
                  val lo = if (ste.MIN_LITERAL() != null) 0L
                           else ste.value().asScala.headOption
                              .map(v => resolveEndpointText(v.getText, 0L)).getOrElse(0L)
                  val hi = if (ste.MAX_LITERAL() != null) Long.MaxValue
                           else ste.value().asScala.lastOption
                              .map(v => resolveEndpointText(v.getText, Long.MaxValue)).getOrElse(Long.MaxValue)
                  Some(SizeConstraint(lo, hi))
                } else if (!ste.value().isEmpty) {
                  val v = resolveEndpointText(ste.value(0).getText, -1L)
                  if (v >= 0) Some(SizeConstraint(v, v)) else None
                } else None
              }
            }
          }.headOption
        }
    }
  }

  private def extractSizeFromConstraint(ctx: ConstraintContext): Option[SizeConstraint] = {
    val cs = ctx.constraintSpec()
    if (cs == null || cs.subtypeConstraint() == null) return None
    findSizeConstraint(cs.subtypeConstraint())
  }
}
