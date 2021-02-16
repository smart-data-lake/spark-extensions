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

import org.apache.spark.sql.Row
import org.scalatest.funsuite.AnyFunSuite

class RowDecoderTest extends AnyFunSuite {

  test("decode row with nested type") {
    val row = Row(Row(true, "test"), Row(true, "test"), 0f, "ok")
    val decoder = new RowDecoder[TestFlags]
    val prfFlags = decoder.convert(row)
    assert(prfFlags == TestFlags(Flag(true,"test"), Flag(true,"test"), 0f, Some("ok")))
  }

  test("decode row with null value") {
    val row = Row(Row(true, "test"), Row(true, "test"), 0f, null)
    val decoder = new RowDecoder[TestFlags]
    val prfFlags = decoder.convert(row)
    assert(prfFlags == TestFlags(Flag(true,"test"), Flag(true,"test"), 0f, None))
  }

  test("decode row with map and nested product") {
    val row = Row("test", Map("flagA" -> Row(true, "resultA")))
    val decoder = new RowDecoder[MapsOfFlags]
    val prfFlags = decoder.convert(row)
    assert(prfFlags == MapsOfFlags("test", Map("flagA"->Flag(true,"resultA"))))
  }
}

case class Flag(x: Boolean, y: String)
case class TestFlags(a: Flag, b:Flag, c:Float, d: Option[String])
case class MapsOfFlags(a: String, b: Map[String,Flag] )