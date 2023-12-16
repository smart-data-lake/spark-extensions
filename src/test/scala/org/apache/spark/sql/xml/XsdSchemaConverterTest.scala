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

package org.apache.spark.sql.xml

import org.apache.spark.sql.types.{ArrayType, DataType, StructType}
import org.scalatest.funsuite.AnyFunSuite

import scala.io.Source

class XsdSchemaConverterTest extends AnyFunSuite {

  test("read basket schema") {
    val xsdContent = Source.fromResource("xmlSchema/basket.xsd").mkString
    val schema = XsdSchemaConverter.read(xsdContent, 10)
  }

  test("read complex schema with recursion") {
    val xsdContent = Source.fromResource("xmlSchema/complex.xsd").mkString
    val schema = XsdSchemaConverter.read(xsdContent, 3)
    // nested lists
    val nodeModifiedArrayType = getNestedElement(schema, Seq("tree","nodes","modified","node")).asInstanceOf[ArrayType]
    val nodeModifiedStructType = nodeModifiedArrayType.elementType.asInstanceOf[StructType]
    // attributeGroup
    assert(nodeModifiedStructType.fieldNames.contains("_validFrom"))
    // recursion
    assert(nodeModifiedStructType.fieldNames.toSeq == nodeModifiedStructType("nodes").dataType.asInstanceOf[StructType]("node").dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType].fieldNames.toSeq)
    // documentation
    nodeModifiedStructType("_nodeType").getComment().contains("Test Documentation")
    // ref
    val nodeDeletedArrayType = getNestedElement(schema, Seq("tree","nodes","deleted","node")).asInstanceOf[ArrayType]
    val nodeDeletedStructType = nodeDeletedArrayType.elementType.asInstanceOf[StructType]
    assert(nodeDeletedStructType.fieldNames.contains("comment"))
  }

  def getNestedElement(schema: StructType, path: Seq[String]): DataType = {
    path.foldLeft[DataType](schema) {
      case (schema: StructType, fieldName) =>
        schema.find(_.name == fieldName).getOrElse(throw new Exception(s"field $fieldName not found in ${schema.fieldNames.mkString(",")}")).dataType
      case (dataType, fieldName) => throw new Exception(s"Cannot extract field $fieldName from non-StructType $dataType")
    }
  }
}
