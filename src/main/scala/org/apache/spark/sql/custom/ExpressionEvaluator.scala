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

import org.apache.spark.sql.catalyst.analysis.{Analyzer, FunctionRegistry}
import org.apache.spark.sql.catalyst.catalog.{CatalogDatabase, InMemoryCatalog, SessionCatalog}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.BindReferences
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, Project}
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.{Column, Encoders}

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

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
  private val dataSerializer = dataEncoder.toRow _
  private val expr = {
    val attributes = dataEncoder.schema.toAttributes
    val localRelation = LocalRelation(attributes)
    val rawPlan = Project(Seq(exprCol.alias("test").named),localRelation)
    val resolvedPlan = ExpressionEvaluator.analyzer.execute(rawPlan).asInstanceOf[Project]
    val resolvedExpr = resolvedPlan.projectList.head
    BindReferences.bindReference(resolvedExpr, attributes)
  }

  // check if expression is fully resolved
  // TODO: try to find unresolved attribute names
  require(expr.resolved, s"expression can not be resolved")

  // prepare result deserializer
  // If result type is any, we just convert types to scala, but there is no decoding into case classes possible.
  val (resultDataType, resultDeserializer) = if (classTagR.runtimeClass != classOf[Any]) {
    val encoder = ExpressionEncoder[R]
    val dataType = encoder.schema.head.dataType
    // check if resulting datatype matches
    require(expr.dataType == dataType, s"expression result data type ${expr.dataType} does not match requested datatype $dataType")
    val resolvedEncoder = encoder.resolveAndBind(encoder.schema.toAttributes)
    val deserializer = (result: Any) => resolvedEncoder.fromRow(InternalRow(result))
    (dataType, deserializer)
  } else {
    val scalaConverter = CatalystTypeConverters.createToScalaConverter(expr.dataType)
    (expr.dataType, (result: Any) => scalaConverter(result).asInstanceOf[R])
  }

  // evaluate expression on object
  def apply(v: T): R = {
    val dataRow = dataSerializer.apply(v)
    val exprResult = expr.eval(dataRow)
    val result = resultDeserializer(exprResult)
    result
  }
}

object ExpressionEvaluator {
  // create a simple catalyst analyzer supporting builtin functions
  private lazy val analyzer: Analyzer = {
    val sqlConf = new SQLConf().copy(SQLConf.CASE_SENSITIVE -> true) // resolve identifiers in expressions case-sensitive
    val simpleCatalog = new SessionCatalog( new InMemoryCatalog, FunctionRegistry.builtin, sqlConf) {
      override def createDatabase(dbDefinition: CatalogDatabase, ignoreIfExists: Boolean): Unit = Unit
    }
    new Analyzer(simpleCatalog, sqlConf)
  }
}