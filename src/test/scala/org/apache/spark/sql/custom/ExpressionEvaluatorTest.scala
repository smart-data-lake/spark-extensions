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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.functions._
import org.scalatest.funsuite.AnyFunSuite

class ExpressionEvaluatorTest extends AnyFunSuite {

  private val input = TestObj(
    s = Seq(Entry("test0",0), Entry("test1",1)),
    m = Map("test2" -> Entry("test2",2), "test3" -> Entry("test3",3)),
    a = 1.5f,
    b = "ok"
  )

  test("evaluate expression with functions") {
    val expression = "concat(b, '-', cast(a * 2 as int))"
    val evaluator = new ExpressionEvaluator[TestObj,String](expr(expression))
    val result = evaluator.apply(input)
    assert(result == "ok-3")
  }

  test("evaluate expression with functions on list") {
    val expression = "array_max(transform( s, entry -> entry.z ))"
    val evaluator = new ExpressionEvaluator[TestObj,Int](expr(expression))
    val result = evaluator.apply(input)
    assert(result == 1)
  }

  test("evaluate expression on map") {
    val expression = "m['test2'].z + m['test3'].z"
    val evaluator = new ExpressionEvaluator[TestObj,Int](expr(expression))
    val result = evaluator.apply(input)
    assert(result == 5)
  }

  test("evaluate expression returning complex type") {
    val expression = "m"
    val evaluator = new ExpressionEvaluator[TestObj,Map[String,Entry]](expr(expression))
    val result = evaluator.apply(input)
    assert(result.values.toSeq.head.z == 2)
  }

  test("evaluate expression with type any") {
    val expression = "a"
    val evaluator = new ExpressionEvaluator[TestObj,Any](expr(expression))
    val result = evaluator.apply(input)
    assert(result == 1.5f)
  }

  test("evaluate expression: exception with unknown attribute contains its name") {
    val expression = "concat(s[0].y, b, abc)"
    val ex = intercept[IllegalArgumentException](new ExpressionEvaluator[TestObj,Any](expr(expression)))
    assert(ex.getMessage.contains("abc"))
  }

  test("evaluate expression: evaluation is case sensitive") {
    val expression = "concat(s[0].Y, b)"
    val ex = intercept[AnalysisException](new ExpressionEvaluator[TestObj,Any](expr(expression)))
    assert(ex.getMessage.contains("Y"))
  }
}

case class Entry(y: String, z: Int)
case class TestObj(s: Seq[Entry], m: Map[String,Entry], a: Float, b: String)
