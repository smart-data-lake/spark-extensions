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

import org.apache.spark.resource.ResourceUtils
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
    schema.printTreeString()
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
    assert(nodeDeletedStructType("comment").nullable)
  }

  test("read fields from basetype and extension") {
    val xsdContent = Source.fromResource("xmlSchema/nestedTypes.xsd").mkString
    val schema = XsdSchemaConverter.read(xsdContent, 10)
    assert(schema.apply("element1").dataType.asInstanceOf[StructType].fieldNames.toSet == Set("a","b","c","_a1","_a2"))
  }

  test("should handle substitutionGroup with external gml schema import") {
    // This test verifies that substitutionGroup references are properly handled
    // The GML schema contains XmlSchemaGroupRef elements that need to be resolved
    val xsdContent = Source.fromResource("xmlSchema/gmltest.xsd").mkString

    // Should now successfully parse the schema
    val schema = XsdSchemaConverter.read(xsdContent, maxRecursion = 10)

    // Verify the schema was created
    assert(schema.fieldNames.contains("abc"))

    // Verify the custom fields are present
    val elementType = schema("abc").dataType.asInstanceOf[StructType]
    assert(elementType.fieldNames.intersect(Seq("a","b","c")).length == 3)
    assert(elementType.fieldNames.length > 3) // gml fields are also present
  }

  test("should handle exclusion of prefixes") {
    // This test verifies that substitutionGroup references are properly handled
    // The GML schema contains XmlSchemaGroupRef elements that need to be resolved
    val xsdContent = Source.fromResource("xmlSchema/gmltest.xsd").mkString

    // Should now successfully parse the schema
    val schema = XsdSchemaConverter.read(xsdContent, maxRecursion = 10, Seq("gml"))

    // Verify the schema was created
    assert(schema.fieldNames.contains("abc"))

    // Verify the custom fields are present
    val elementType = schema("abc").dataType.asInstanceOf[StructType]
    assert(elementType.fieldNames.toSet == Set("a","b","c"))
  }

  def getNestedElement(schema: StructType, path: Seq[String]): DataType = {
    path.foldLeft[DataType](schema) {
      case (schema: StructType, fieldName) =>
        schema.find(_.name == fieldName).getOrElse(throw new Exception(s"field $fieldName not found in ${schema.fieldNames.mkString(",")}")).dataType
      case (dataType, fieldName) => throw new Exception(s"Cannot extract field $fieldName from non-StructType $dataType")
    }
  }
}
