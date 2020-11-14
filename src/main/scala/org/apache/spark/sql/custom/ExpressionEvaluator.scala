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

import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.catalyst.analysis.{Analyzer, FakeV2SessionCatalog, FunctionRegistry}
import org.apache.spark.sql.catalyst.catalog.{CatalogDatabase, InMemoryCatalog, SessionCatalog}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.BindReferences
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, Project}
import org.apache.spark.sql.connector.catalog.CatalogManager
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.{Column, Encoders}

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

/**
 * ExpressionEvaluator can evaluate a Spark SQL expression against a case class
 *
 * @param exprCol expression to be evaluated as Column object
 * @tparam T class of object the expression should be evaluated on
 * @tparam R class of expressions expected return type
 */
class ExpressionEvaluator[T<:Product:TypeTag,R:TypeTag](exprCol: Column)(implicit classTagR: ClassTag[R]) {

  // prepare evaluator (this is Spark internal API)
  private val encoder = Encoders.product[T].asInstanceOf[ExpressionEncoder[T]]
  private val rowSerializer = encoder.createSerializer()
  private val expr = {
    val attributes = encoder.schema.toAttributes
    val localRelation = LocalRelation(attributes)
    val rawPlan = Project(Seq(exprCol.alias("test").named),localRelation)
    val resolvedPlan = ExpressionEvaluator.analyzer.execute(rawPlan).asInstanceOf[Project]
    val resolvedExpr = resolvedPlan.projectList.head
    BindReferences.bindReference(resolvedExpr, attributes)
  }
  private val resultDataType = ExpressionEncoder[R].schema.head.dataType
  private val scalaConverter = CatalystTypeConverters.createToScalaConverter(resultDataType)

  // check if expression is fully resolved
  require(expr.resolved, s"expression can not be resolved")

  // check if resulting datatype matches
  if (classTagR.runtimeClass != classOf[Any]) {
    require(expr.dataType == resultDataType, s"expression result data type ${expr.dataType} does not match requested datatype $resultDataType")
  }

  // evaluate expression on object
  def apply(v: T): R = {
    val row = rowSerializer.apply(v)
    val result = scalaConverter(expr.eval(row))
    result.asInstanceOf[R]
  }
}

object ExpressionEvaluator {
  // create a simple catalyst analyzer supporting builtin functions
  private lazy val analyzer: Analyzer = {
    val sqlConf = new SQLConf().copy(SQLConf.CASE_SENSITIVE -> true) // resolve identifiers in expressions case-sensitive
    val simpleCatalog = new SessionCatalog( new InMemoryCatalog, FunctionRegistry.builtin, sqlConf) {
      override def createDatabase(dbDefinition: CatalogDatabase, ignoreIfExists: Boolean): Unit = Unit
    }
    new Analyzer(new CatalogManager(sqlConf, FakeV2SessionCatalog, simpleCatalog), sqlConf)
  }
}