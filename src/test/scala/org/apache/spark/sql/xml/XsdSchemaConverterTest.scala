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
    val nodeArrayType = extractSubDataType(schema, "node").map(_.asInstanceOf[ArrayType]).get
    val nodeStructType = nodeArrayType.elementType.asInstanceOf[StructType]
    // attributeGroup
    assert(nodeStructType.fieldNames.contains("_validFrom"))
    // recursion
    assert(nodeStructType.fieldNames.toSeq == nodeStructType("nodes").dataType.asInstanceOf[StructType]("node").dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType].fieldNames.toSeq)
    // documentation
    nodeStructType("_nodeType").getComment().contains("Test Documentation")
  }

  def extractSubDataType(schema: StructType, elementName: String): Option[DataType] = {
    schema.fields.find(_.name == elementName).map(_.dataType)
      .orElse {
        val structFields = schema.fields.map(_.dataType).collect{
          case x: StructType => x
          case x: ArrayType if x.elementType.isInstanceOf[StructType] => x.elementType.asInstanceOf[StructType]
        }
        structFields.flatMap(x => extractSubDataType(x, elementName)).headOption
      }
  }
}
