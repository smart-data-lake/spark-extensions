/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.custom

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.analysis.{Analyzer, FakeV2SessionCatalog, FunctionRegistry, Resolver, UnresolvedAttribute, caseSensitiveResolution}
import org.apache.spark.sql.catalyst.catalog.{CatalogDatabase, ExternalCatalog, InMemoryCatalog, SessionCatalog}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.{BindReferences, Expression}
import org.apache.spark.sql.catalyst.optimizer.{ComputeCurrentTime, ReplaceCurrentLike, ReplaceExpressions, ReplaceUpdateFieldsExpression}
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.connector.catalog.CatalogManager
import org.apache.spark.sql.custom.ExpressionEvaluator.{findUnresolvedAttributes, resolveExpression}
import org.apache.spark.sql.expressions.{UserDefinedAggregator, UserDefinedFunction}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{Column, Encoders}

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._
import scala.util.Try

/**
 * ExpressionEvaluator can evaluate a Spark SQL expression against a case class
 *
 * @param exprCol expression to be evaluated as Column object
 * @tparam T class of object the expression should be evaluated on
 * @tparam R class of expressions expected return type. This might also be set Any, in that case the result type check is
 *           omitted and complex datatypes will not be mapped to case classes, as they are not specified.
 */
class ExpressionEvaluator[T<:Product:TypeTag,R:TypeTag](exprCol: Column)(implicit classTagR: ClassTag[R]) {

  // prepare evaluator (this is Spark internal API)
  private val dataEncoder = Encoders.product[T].asInstanceOf[ExpressionEncoder[T]]
  private val dataSerializer = dataEncoder.createSerializer
  private val expr = resolveExpression(exprCol, dataEncoder.schema)

  // check if expression is fully resolved
  require(expr.resolved, {
    val attrs = findUnresolvedAttributes(expr).map(_.name)
    "expression can not be resolved" + (if (attrs.nonEmpty) s", unresolved attributes are ${attrs.mkString(", ")}" else "")
  })

  // prepare result deserializer
  // If result type is any, we just convert types to scala, but there is no decoding into case classes possible.
  val (resultDataType, resultDeserializer) = if (classTagR.runtimeClass != classOf[Any]) {
    val encoder = ExpressionEncoder[R]
    val dataType = encoder.schema.head.dataType
    // check if resulting datatype matches
    require(DataType.equalsStructurally(expr.dataType, dataType, ignoreNullability = true), s"expression result data type ${expr.dataType} does not match requested datatype $dataType")
    val resolvedEncoder = encoder.resolveAndBind(encoder.schema.toAttributes)
    val deserializer = (result: Any) => resolvedEncoder.createDeserializer()(InternalRow(result))
    (dataType, deserializer)
  } else {
    val scalaConverter = CatalystTypeConverters.createToScalaConverter(expr.dataType)
    (expr.dataType, (result: Any) => scalaConverter(result).asInstanceOf[R])
  }

  // evaluate expression on object
  def apply(v: T): R = {
    val dataRow = dataSerializer(v)
    val exprResult = expr.eval(dataRow)
    val result = resultDeserializer(exprResult)
    result
  }
}

object ExpressionEvaluator extends Logging {

  // keep our own function registry
  private lazy val functionRegistry = FunctionRegistry.builtin.clone()

  // create a simple catalyst analyzer and optimizer rule list supporting builtin functions
  private lazy val (analyzer, optimizerRules): (Analyzer, Seq[Rule[LogicalPlan]]) = {
    val sqlConf = new SQLConf()
    sqlConf.setConf(SQLConf.CASE_SENSITIVE, true) // resolve identifiers in expressions case-sensitive
    val externalCatalog = new InMemoryCatalog
    // Databricks has a modified Spark 3.1/3.2 Version, we try to create original catalog manager and the databricks version while catching exception to report them later.
    val originalCatalogManager = try {
      val simpleCatalog = new SessionCatalog(externalCatalog, functionRegistry, sqlConf) {
        override def createDatabase(dbDefinition: CatalogDatabase, ignoreIfExists: Boolean): Unit = Unit
      }
      Right(new CatalogManager(FakeV2SessionCatalog, simpleCatalog))
    } catch {
      // NoSuchMethodError extends Throwable directly, which is not caught by Try...
      case t: Throwable => Left(t)
    }
    val databricksCatalogManager = try {
      Right(createDatabricksCatalogManager(externalCatalog, sqlConf))
    } catch {
      case t: Throwable => Left(t)
    }
    val catalogManager = originalCatalogManager.toTry
      .orElse(databricksCatalogManager.toTry)
      .getOrElse{
        logError("Exception for Spark original API", originalCatalogManager.left.get)
        logError("Exception for Databricks modified API", databricksCatalogManager.left.get)
        throw new RuntimeException("Could not create SessionCatalog")
      }
    val analyzer = new Analyzer(catalogManager) {
      override def resolver: Resolver = caseSensitiveResolution // resolve identifiers in expressions case-sensitive
    }
    // only apply a small selection of optimizer rules needed to evaluate simple expressions.
    val optimizerRules = Seq(ReplaceExpressions, ComputeCurrentTime, ReplaceCurrentLike(catalogManager), ReplaceUpdateFieldsExpression)
    (analyzer, optimizerRules)
  }

  /**
   * Databricks has a modified Spark Version, because of integration of their Unity-catalog.
   * To create a CatalogManager, an instance of the Databricks specific SessionCatalogImpl must be created dynamically.
   */
  private def createDatabricksCatalogManager(catalog: ExternalCatalog, sqlConf: SQLConf): CatalogManager = {
    val clazzSessionCatalog = this.getClass.getClassLoader.loadClass("org.apache.spark.sql.catalyst.catalog.SessionCatalogImpl")
    val constructorSessionCatalog = clazzSessionCatalog.getConstructors
      .find(_.getParameterTypes.toSeq == Seq(classOf[ExternalCatalog], classOf[FunctionRegistry], classOf[SQLConf]))
      .get
    val simpleCatalog = constructorSessionCatalog.newInstance(catalog, functionRegistry, sqlConf).asInstanceOf[SessionCatalog]
    val clazzCatalogManager = this.getClass.getClassLoader.loadClass("org.apache.spark.sql.connector.catalog.CatalogManager")
    val constructorCatalogManager = clazzCatalogManager.getConstructors.head
    val catalogManager = constructorCatalogManager.newInstance(FakeV2SessionCatalog, simpleCatalog, Boolean.box(false) /* unityCatalogEnv */).asInstanceOf[CatalogManager]
    //return
    catalogManager
  }

  /**
   * Register a udf to be available in evaluating expressions.
   *
   * Note: this code is copied from Spark UDFRegistration.register
   */
  def registerUdf(name: String, udf: UserDefinedFunction): Unit = {
    udf match {
      case udaf: UserDefinedAggregator[_, _, _] =>
        def builder(children: Seq[Expression]) = udaf.scalaAggregator(children)
        functionRegistry.createOrReplaceTempFunction(name, builder, "scala_udf")
      case _ =>
        def builder(children: Seq[Expression]) = udf.apply(children.map(Column.apply) : _*).expr
        functionRegistry.createOrReplaceTempFunction(name, builder, "scala_udf")
    }
  }

  /**
   * Resolve an expression against a given schema.
   * A resolved expression has a dataType and can be evaluated against data.
   */
  def resolveExpression(exprCol: Column, schema: StructType, caseSensitive: Boolean = true): Expression = {
    val schemaPrep = if (caseSensitive) schema
    else StructType(schema.map(f => f.copy(name = f.name.toLowerCase)))
    val attributes = schemaPrep.toAttributes
    val localRelation = LocalRelation(attributes)
    val rawPlan = Project(Seq(exprCol.alias("test").named),localRelation)
    val resolvedPlan = analyzer.execute(rawPlan)
    val optimizedPlan = optimizerRules.foldLeft(resolvedPlan) {
      case (plan, rule) => rule.apply(plan)
    }
    val resolvedExpr = optimizedPlan.asInstanceOf[Project].projectList.head
    BindReferences.bindReference(resolvedExpr, attributes)
  }

  /**
   * Search for unresolved attributes in an expression to create meaningful error messages.
   */
  def findUnresolvedAttributes(expr: Expression): Seq[UnresolvedAttribute] = {
    if (expr.resolved) Seq()
    else expr match {
      case attr: UnresolvedAttribute => Seq(attr)
      case _ => expr.children.flatMap(findUnresolvedAttributes)
    }
  }
}