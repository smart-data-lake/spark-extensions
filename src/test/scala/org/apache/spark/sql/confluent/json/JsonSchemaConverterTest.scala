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
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.io.Source
import scala.util.Using

/**
 * This code originates from https://github.com/zalando-incubator/spark-json-schema and is protected by its corresponding MIT license.
 */
class JsonSchemaConverterTest extends AnyFunSuite with Matchers with BeforeAndAfter {

  private val expectedStruct = StructType(Array(
    StructField("object", StructType(Array(
      StructField("item1", StringType),
      StructField("item2", StringType)
    ))),
    StructField("array", ArrayType(StructType(Array(
      StructField("itemProperty1", StringType),
      StructField("itemProperty2", DecimalType(38,18))
    )), containsNull = false)),
    StructField("structure", StructType(Array(
      StructField("nestedArray", ArrayType(StructType(Array(
        StructField("key", StringType),
        StructField("value", LongType)
      )), containsNull = false))
    ))),
    StructField("integer", LongType),
    StructField("string", StringType),
    StructField("number", DecimalType(38,18)),
    StructField("floatRequired", DoubleType, nullable = false),
    StructField("nullable", DecimalType(38,18)),
    StructField("booleanWithComment", BooleanType).withComment("todo"),
    StructField("additionalProperty", StringType)
  ))

  private val jsonSparkTypeMap = Map(
    "string" -> StringType,
    "number" -> DecimalType(38,18),
    "float" -> DoubleType,
    "integer" -> LongType,
    "boolean" -> BooleanType
  )

  test("should convert schema.json into spark StructType") {
    val testSchema = JsonSchemaConverter.convertToSpark(getTestResourceContent("/jsonSchema/testJsonSchema.json"))
    assert(testSchema === expectedStruct)
  }

  // 'id' and 'name' are optional according to http://json-schema.org/latest/json-schema-core.html
  test("should support optional 'id' and 'name' properties") {
    val testSchema = JsonSchemaConverter.convertToSpark(getTestResourceContent("/jsonSchema/testJsonSchema3.json"))
    assert(testSchema === expectedStruct)
  }

  test("schema should support references") {
    val schema = JsonSchemaConverter.convertToSpark(getTestResourceContent("/jsonSchema/testJsonSchema4.json"))

    val expected = StructType(Array(
      StructField("name", StringType, nullable = false),
      StructField("addressA", StructType(Array(
        StructField("zip", StringType, nullable = true)
      )), nullable = false),
      StructField("addressB", StructType(Array(
        StructField("zip", StringType, nullable = true)
      )), nullable = false)
    ))

    assert(schema === expected)
  }

  test("Empty object should be possible") {
    val schema = JsonSchemaConverter.convertToSpark(
      """
        {
          "$schema": "smallTestSchema",
          "type": "object",
          "properties": {
            "address": {
              "type": "object",
              "additionalProperties": true
            }
          }
        }
      """
    )
    val expected = StructType(Array(
      StructField("address", MapType(StringType,StringType), nullable = true)
    ))

    assert(schema === expected)
  }

  test("Known primitive type array should be an array of this type") {
    jsonSparkTypeMap.foreach {
      case p @ (key, datatype) =>
        val schema = JsonSchemaConverter.convertToSpark(
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

  test("Array of array should be an array of array") {

    val schema = JsonSchemaConverter.convertToSpark(
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

  test("Array of object should be an array of object") {

    val schema = JsonSchemaConverter.convertToSpark(
      """
          {
            "$$schema": "smallTestSchema",
            "type": "object",
            "properties": {
              "array" : {
                "type" : "array",
                "items": {
                  "type": "object",
                  "additionalProperties": true
                }
              }
            }
          }
        """
    )
    val expected = StructType(Array(
      StructField("array", ArrayType(MapType(StringType, StringType), containsNull = false), nullable = true)
    ))

    assert(schema === expected)
  }

  test("Array of object with properties should be an array of object with these properties") {

    val schema = JsonSchemaConverter.convertToSpark(
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

  test("Array of unknown type should fail") {

    assertThrows[IllegalStateException] {
      val schema = JsonSchemaConverter.convertToSpark(
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

  test("Array of various type should be merged") {
    val schema = JsonSchemaConverter.convertToSpark(
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
    val expected = StructType(Array(
      StructField("array", ArrayType(StringType, containsNull = false), nullable = true)
    ))

    assert(schema === expected)
  }

  test("Array of various object type should be merged") {
    val schema = JsonSchemaConverter.convertToSpark(
      """
      {
        "$$schema": "smallTestSchema",
        "type": "object",
        "properties": {
          "array" : {
            "type" : "array",
            "items" : {
              "type" : [{
                  "type": "object",
                  "properties" : {
                    "prop1" : {
                      "type" : "string"
                    }
                  }
                }, {
                  "type": "object",
                  "properties" : {
                    "prop2" : {
                      "type" : "string"
                    }
                  }
                }]
            }
          }
        }
      }
    """
    )
    val expected = StructType(Array(
      StructField("array", ArrayType(StructType(Seq(StructField("prop1",StringType),StructField("prop2",StringType))), containsNull = false), nullable = true)
    ))

    assert(schema === expected)
  }

  test("Array of nullable type should be an array of nullable type") {
    jsonSparkTypeMap.foreach {
      case (name, atype) =>
        val schema = JsonSchemaConverter.convertToSpark(
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
          StructField("array", ArrayType(atype, containsNull = true))
        ))

        assert(schema === expected)
    }
  }

  test("Array of non-nullable type should be an array of non-nullable type") {
    val schema = JsonSchemaConverter.convertToSpark(
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
      StructField("array", ArrayType(StringType, containsNull = false))
    ))

    assert(schema === expected)
  }

  test("Array of nullable object should be an array of nullable object") {
    val schema = JsonSchemaConverter.convertToSpark(
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
                  },
                  "required": ["prop"]
                }
              }
            }
          }
        """
    )

    val expected = StructType(Array(
      StructField("array", ArrayType(
        StructType(Seq(StructField("prop", StringType, nullable = false))), containsNull = true
      ))
    ))

    assert(schema === expected)
  }

  test("Nullable array should be an array or a null value") {
    val schema = JsonSchemaConverter.convertToSpark(
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

  test("Multiple types should fail with strict typing") {
    assertThrows[IllegalArgumentException] {
      val schema = JsonSchemaConverter.convertToSpark(
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

  test("Multiple types should default to string without strict typing") {
    val schema = JsonSchemaConverter.convertToSpark(
      """
          {
            "$$schema": "smallTestSchema",
            "type": "object",
            "properties": {
              "prop" : {
                "type" : ["integer", "float"]
              }
            },
            "required": ["prop"],
            "additionalProperties": false
          }
        """,
      isStrictTypingEnabled = false
    )

    val expected = StructType(Array(
      StructField("prop", StringType, nullable = false)
    ))

    assert(schema === expected)
  }

  test("null type only should fail") {
    assertThrows[NoSuchElementException] {
      val schema = JsonSchemaConverter.convertToSpark(
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

  test("null type only should fail event as a single array element") {
    assertThrows[IllegalArgumentException] {
      val schema = JsonSchemaConverter.convertToSpark(
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


  test("Object with oneOf type should be merged") {
    val schema = JsonSchemaConverter.convertToSpark(
      """
        {
          "$$schema": "smallTestSchema",
          "oneOf": [{
            "type": "object",
            "properties" : {
              "prop1" : {
                "type" : "string"
              }
            }
          }, {
            "type": "object",
            "properties" : {
              "prop2" : {
                "type" : "string"
              }
            },
            "required": ["prop2"]
          }]
        }
      """
    )
    val expected = StructType(Seq(StructField("prop1", StringType), StructField("prop2", StringType)))

    assert(schema === expected)
  }

  def getTestResourceContent(relativePath: String): String = {
    Option(getClass.getResource(relativePath)) match {
      case Some(relPath) => Using.resource(Source.fromURL(relPath))(_.mkString)
      case None => throw new IllegalArgumentException(s"Path can not be reached: $relativePath")
    }
  }
}