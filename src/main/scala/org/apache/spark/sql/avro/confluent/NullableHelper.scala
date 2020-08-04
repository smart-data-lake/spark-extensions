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

package org.apache.spark.sql.avro.confluent

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Expression, UnaryExpression}
import org.apache.spark.sql.types.DataType

case class SetNullable(child: Expression, forcedNullable: Boolean) extends UnaryExpression {
  override def dataType: DataType = child.dataType
  override def nullable: Boolean = forcedNullable // override nullable
  override def nullSafeEval(input: Any): Any = input
  override def prettyName: String = "set_nullable"
  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val expr = ctx.addReferenceObj("this", this)
    defineCodeGen(ctx, ev, input => s"(${CodeGenerator.boxedType(dataType)})$expr.nullSafeEval($input)")
  }
}

object NullableHelper {
  def makeNotNullable(data: Column): Column = {
    new Column(SetNullable(data.expr, false))
  }
  def makeNullable(data: Column): Column = {
    new Column(SetNullable(data.expr, true))
  }
}