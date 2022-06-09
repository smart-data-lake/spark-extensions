/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2021 ELCA Informatique SA (<https://www.elca.ch>)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package org.apache.spark.sql.confluent.json

import org.apache.spark.sql.types._
import org.json4s.{DefaultFormats, Formats, JObject}
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.io.Source

/**
 * This code originates from https://github.com/zalando-incubator/spark-json-schema and is protected by its corresponding MIT license.
 */
class JsonSchemaConverterTest extends AnyFunSuite with Matchers with BeforeAndAfter {

  val expectedStruct = StructType(Seq(
    StructField("object", StructType(Seq(
      StructField("item1", StringType, nullable = false),
      StructField("item2", StringType, nullable = false)
    )), nullable = false),
    StructField("array", ArrayType(StructType(Seq(
      StructField("itemProperty1", StringType, nullable = true),
      StructField("itemProperty2", DoubleType, nullable = true)
    )), containsNull = false), nullable = false),
    StructField("structure", StructType(Seq(
      StructField("nestedArray", ArrayType(StructType(Seq(
        StructField("key", StringType, nullable = true),
        StructField("value", LongType, nullable = true)
      )), containsNull = false), nullable = true)
    )), nullable = false),
    StructField("integer", LongType, nullable = false),
    StructField("string", StringType, nullable = false),
    StructField("number", DoubleType, nullable = false),
    StructField("nullable", DoubleType, nullable = true),
    StructField("boolean", BooleanType, nullable = false),
    StructField("additionalProperty", StringType, nullable = true),
    StructField("map", MapType(StringType, DoubleType), nullable = true)
  ))
  
  private def convertToSparkStrict(schemaContent: String) = JsonSchemaConverter.convertToSpark(schemaContent, isStrictTypingEnabled = true)
  private def convertToSparkNonStrict(schemaContent: String) = JsonSchemaConverter.convertToSpark(schemaContent, isStrictTypingEnabled = false)

  test("should convert schema.json into spark StructType") {
    val testSchema = convertToSparkStrict(getTestResourceContent("/jsonSchema/testJsonSchemaVerbose.json"))
    assert(testSchema === expectedStruct)
  }

  test("should convert schema.json content into spark StructType") {
    val testSchema = convertToSparkStrict(getTestResourceContent("/jsonSchema/testJsonSchemaVerbose.json"))
    assert(testSchema === expectedStruct)
  }

  // 'id' and 'name' are optional according to http://json-schema.org/latest/json-schema-core.html
  test("json schema should support optional 'id' and 'name' properties") {
    val testSchema = convertToSparkStrict(getTestResourceContent("/jsonSchema/testJsonSchemaSlim.json"))
    assert(testSchema === StructType(expectedStruct.filterNot(_.name == "nullable")))
  }

  test("json schema schema should support references") {
    val schema = convertToSparkStrict(getTestResourceContent("/jsonSchema/testJsonSchemaRefs.json"))

    val expected = StructType(Array(
      StructField("name", StringType, nullable = true),
      StructField("addressA", StructType(Array(
        StructField("zip", StringType, nullable = true)
      )), nullable = true),
      StructField("addressB", StructType(Array(
        StructField("zip", StringType, nullable = true)
      )), nullable = true)
    ))

    assert(schema === expected)
  }

  test("json schema empty object should be possible") {
    val schema = convertToSparkStrict(
      """
        {
          "$schema": "smallTestSchema",
          "type": "object",
          "properties": {
            "address": {
              "type": "object"
            }
          }
        }
      """
    )
    val expected = StructType(Array(
      StructField("address", StructType(Seq.empty), nullable = true)
    ))

    assert(schema === expected)
  }

  test("json schema known primitive type array should be an array of this type") {
    val typeMap = Map(
      "string" -> StringType,
      "number" -> DoubleType,
      "integer" -> LongType,
      "boolean" -> BooleanType
    )
    typeMap.foreach {
      case p @ (key, datatype) =>
        val schema = convertToSparkStrict(
          s"""
          {
            "$$schema": "smallTestSchema",
            "type": "object",
            "properties": {
              "array" : {
                "type" : "array",
                "items": {
                  "type": "$key"
                }
              }
            }
          }
        """
        )
        val expected = StructType(Array(
          StructField("array", ArrayType(datatype, containsNull = false), nullable = true)
        ))

        assert(schema === expected)
    }
  }

  test("json schema array of array should be an array of array") {
    val schema = convertToSparkStrict(
      """
          {
            "$$schema": "smallTestSchema",
            "type": "object",
            "properties": {
              "array" : {
                "type" : "array",
                "items": {
                  "type": "array",
                  "items": {
                    "type": "string"
                  }
                }
              }
            }
          }
        """
    )
    val expected = StructType(Array(
      StructField("array", ArrayType(ArrayType(StringType, containsNull = false), containsNull = false), nullable = true)
    ))

    assert(schema === expected)
  }

  test("json schema array of object should be an array of object") {
    val schema = convertToSparkStrict(
      """
          {
            "$$schema": "smallTestSchema",
            "type": "object",
            "properties": {
              "array" : {
                "type" : "array",
                "items": {
                  "type": "object"
                }
              }
            }
          }
        """
    )
    val expected = StructType(Array(
      StructField("array", ArrayType(StructType(Seq.empty), containsNull = false), nullable = true)
    ))

    assert(schema === expected)
  }

  test("json schema array of object with properties should be an array of object with these properties") {
    val schema = convertToSparkStrict(
      """
          {
            "$$schema": "smallTestSchema",
            "type": "object",
            "properties": {
              "array" : {
                "type" : "array",
                "items": {
                  "type": "object",
                  "properties": {
                    "name" : {
                      "type" : "string"
                    }
                  }
                }
              }
            }
          }
        """
    )
    val expected = StructType(Array(
      StructField("array", ArrayType(StructType(Seq(StructField("name", StringType, nullable = true))), containsNull = false), nullable = true)
    ))

    assert(schema === expected)
  }

  test("json schema array of unknown type should fail") {
    assertThrows[IllegalArgumentException] {
      val schema = convertToSparkStrict(
        """
          {
            "$$schema": "smallTestSchema",
            "type": "object",
            "properties": {
              "array" : {
                "type" : "array",
                "items" : {}
              }
            }
          }
        """
      )
    }
  }

  test("json schema array of various type should fail") {
    assertThrows[IllegalArgumentException] {
      val schema = convertToSparkStrict(
        """
          {
            "$$schema": "smallTestSchema",
            "type": "object",
            "properties": {
              "array" : {
                "type" : "array",
                "items" : {
                  "type" : ["string", "integer"]
                }
              }
            }
          }
        """
      )
    }
  }

  test("json schema array of nullable type should be an array of nullable type") {
    val typeMap = Map(
      "string" -> StringType,
      "number" -> DoubleType,
      "integer" -> LongType,
      "boolean" -> BooleanType
    )
    typeMap.foreach {
      case (name, atype) =>
        val schema = convertToSparkStrict(
          s"""
          {
            "$$schema": "smallTestSchema",
            "type": "object",
            "properties": {
              "array" : {
                "type" : "array",
                "items" : {
                  "type" : ["$name", "null"]
                }
              }
            }
          }
        """
        )

        val expected = StructType(Array(
          StructField("array", ArrayType(atype, containsNull = true), nullable = true)
        ))

        assert(schema === expected)
    }
  }

  test("json schema array of non-nullable type should be an array of non-nullable type") {
    val schema = convertToSparkStrict(
      """
          {
            "$$schema": "smallTestSchema",
            "type": "object",
            "properties": {
              "array" : {
                "type" : "array",
                "items" : {
                  "type" : ["string"]
                }
              }
            }
          }
        """
    )

    val expected = StructType(Array(
      StructField("array", ArrayType(StringType, containsNull = false), nullable = true)
    ))

    assert(schema === expected)
  }

  test("json schema array of nullable object should be an array of nullable object") {
    val schema = convertToSparkStrict(
      """
          {
            "$$schema": "smallTestSchema",
            "type": "object",
            "properties": {
              "array" : {
                "type" : "array",
                "items" : {
                  "type" : ["object", "null"],
                  "properties" : {
                    "prop" : {
                      "type" : "string"
                    }
                  }
                }
              }
            }
          }
        """
    )

    val expected = StructType(Array(
      StructField("array", ArrayType(
        StructType(Seq(StructField("prop", StringType, nullable = true))), containsNull = true
      ), nullable = true)
    ))

    assert(schema === expected)
  }

  test("json schema nullable array should be an array or a null value") {
    val schema = convertToSparkStrict(
      """
          {
            "$$schema": "smallTestSchema",
            "type": "object",
            "properties": {
              "array" : {
                "type" : ["array", "null"],
                "items" : {
                  "type" : "string"
                }
              }
            }
          }
        """
    )

    val expected = StructType(Array(
      StructField("array", ArrayType(StringType, containsNull = false), nullable = true)
    ))

    assert(schema === expected)
  }

  test("json schema multiple types should fail with strict typing") {
    assertThrows[IllegalArgumentException] {
      val schema = convertToSparkStrict(
        """
          {
            "$$schema": "smallTestSchema",
            "type": "object",
            "properties": {
              "prop" : {
                "type" : ["integer", "float"]
              }
            }
          }
        """
      )

    }
  }

  test("json schema multiple types should default to string without strict typing") {
    val schema = convertToSparkNonStrict(
      """
          {
            "$$schema": "smallTestSchema",
            "type": "object",
            "properties": {
              "prop" : {
                "type" : ["integer", "float"]
              }
            }
          }
        """
    )

    val expected = StructType(Array(
      StructField("prop", StringType, nullable = true)
    ))

    assert(schema === expected)
  }

  test("json schema null type only should fail") {
    assertThrows[NoSuchElementException] {
      val schema = convertToSparkStrict(
        """
          {
            "$$schema": "smallTestSchema",
            "type": "object",
            "properties": {
              "prop" : {
                "type" : "null"
              }
            }
          }
        """
      )
    }
  }

  test("json schema null type only should fail event as a single array element") {
    assertThrows[IllegalArgumentException] {
      val schema = convertToSparkStrict(
        """
          {
            "$$schema": "smallTestSchema",
            "type": "object",
            "properties": {
              "prop" : {
                "type" : ["null"]
              }
            }
          }
        """
      )
    }
  }

  test("json schema should support creating map from additionalProperties") {
    val schema = convertToSparkStrict(getTestResourceContent("/jsonSchema/testJsonSchemaMap.json"))

    val expected = StructType(Array(
      StructField("strictMap", MapType(StringType, DoubleType), nullable = true),
      StructField("nonStrictMap", MapType(StringType, StringType), nullable = true),
    ))

    assert(schema === expected)
  }

  test("slim json schema converted to spark and back should be equal") {
    import org.json4s.jackson.JsonMethods._
    implicit val format: Formats = DefaultFormats
    val originalJsonSchemaContent = getTestResourceContent("/jsonSchema/testJsonSchemaSlim.json")
    val originalJsonSchema = parse(originalJsonSchemaContent).extract[JObject]

    val sparkSchema = JsonSchemaConverter.convertToSpark(originalJsonSchema, true)
    val restoredJsonSchema = JsonSchemaConverter.convertFromSpark(sparkSchema)

    assert(restoredJsonSchema == originalJsonSchema)
  }


  def getTestResourceContent(relativePath: String): String = {
    Option(getClass.getResource(relativePath)) match {
      case Some(relPath) =>
        val src = Source.fromURL(relPath)
        val content = src.mkString
        src.close
        content
      case None => throw new IllegalArgumentException(s"Path can not be reached: $relativePath")
    }
  }
}