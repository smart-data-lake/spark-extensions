package org.apache.spark.sql.confluent.json

import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StringType, StructField, StructType}
import org.json4s.JsonAST.JBool
import org.json4s.{DefaultFormats, Formats, JArray, JNothing, JNull, JObject, JString, JValue}

/**
 * This code originates from https://github.com/zalando-incubator/spark-json-schema and is protected by its corresponding MIT license.
 * We added support for date-time datatype, and changed from using play-json library to json4s to minimize SDL dependencies.
 *
 * Motivation for copying vs creating a PR: It seems that the original version isn't maintained very well. An open PR exists for
 * timestamp and decimal datatype support. It is open for nearly one year, see https://github.com/zalando-incubator/spark-json-schema/pull/49.
 * Also to change from play-json to json4s would be hard to argument on the original library.
 */

/**
 * Schema Converter for getting schema in json format into a spark Structure
 *
 * The given schema for spark has almost no validity checks, so it will make sense
 * to combine this with the schema-validator. For loading data with schema, data is converted
 * to the type given in the schema. If this is not possible the whole row will be null (!).
 * A field can be null if its type is a 2-element array, one of which is "null". The converted
 * schema doesn't check for 'enum' fields, i.e. fields which are limited to a given set.
 * It also doesn't check for required fields or if additional properties are set to true
 * or false. If a field is specified in the schema, than you can select it and it will
 * be null if missing. If a field is not in the schema, it cannot be selected even if
 * given in the dataset.
 */
class JsonToSparkSchemaConverter(inputSchema: JObject, isStrictTypingEnabled: Boolean) {
  import JsonSchemaConverter._
  implicit val format: Formats = DefaultFormats

  lazy val definitions: JObject = (inputSchema \ Definitions).extractOpt[JObject].getOrElse(JObject())

  def convert(): StructType = {
    val name = getJsonName(inputSchema).getOrElse(SchemaRoot)
    val typeName = getJsonType(inputSchema, name).typeName
    if (name == SchemaRoot && typeName == "object") {
      val properties = (inputSchema \ SchemaObjectProperties).extractOpt[JObject]
        .getOrElse(throw new NoSuchElementException(s"Root level of schema needs to have a [$SchemaObjectProperties]-field"))
      val required = (inputSchema \ SchemaObjectPropertiesRequired).extractOpt[Seq[String]].getOrElse(Seq())
      toSparkStruct(properties.obj, required)
    } else {
      throw new IllegalArgumentException(s"Schema needs root level called <$SchemaRoot> and root type <object>. Current root is <$name> and type is <$typeName>")
    }
  }

  private def getJsonName(json: JValue): Option[String] = (json \ SchemaFieldName).extractOpt[String]

  private def getJsonId(json: JValue): Option[String] = (json \ SchemaFieldId).extractOpt[String]

  private def getJsonType(json: JObject, name: String): SchemaType = {
    val id = getJsonId(json).getOrElse(name)

    (json \ SchemaFieldType) match {
      case JString(s) => SchemaType(s.trim, nullable = false)
      case JArray(array) =>
        val nullable = array.contains(JString("null"))
        array.size match {
          case 1 if nullable =>
            throw new IllegalArgumentException(s"Null type only is not supported at <$id>")
          case 1 =>
            SchemaType(array.head.extract[String], nullable = nullable)
          case 2 if nullable =>
            array.find(_ != JString("null"))
              .map(i => SchemaType(i.extract[String], nullable = nullable))
              .getOrElse(throw new IllegalArgumentException(s"Incorrect definition of a nullable parameter at <$id>"))
          case _ if isStrictTypingEnabled =>
            throw new IllegalArgumentException(s"Unsupported type definition <${array.toString}> in schema at <$id>")
          case _ => // Default to string as it is the "safest" type
            SchemaType("string", nullable = nullable)
        }
      case JNull =>
        throw new IllegalArgumentException(s"No <$SchemaFieldType>-field in schema at <$id>")
      case t =>
        throw new IllegalArgumentException(s"Unsupported type <${t.toString}> in schema at <$id>")
    }
  }

  private def resolveRefs(inputJson: JObject): JObject = {
    val schemaRef = (inputJson \ Reference).extractOpt[String]
    schemaRef match {
      case Some(loc) =>
        val searchDefinitions = Definitions + "/"
        val defIndex = loc.indexOf(searchDefinitions) match {
          case -1 => throw new NoSuchElementException(
            s"Field with name [$Reference] requires path with [$searchDefinitions]"
          )
          case i: Int => i + searchDefinitions.length
        }
        val pathNodes = loc.drop(defIndex).split("/").toList
        val definition = pathNodes.foldLeft(definitions: JValue){ case (obj, node) => obj \ node} match {
          case obj: JObject => obj
          case JNothing => throw new NoSuchElementException(s"Path [$loc] not found in $Definitions")
          case _ => throw new NoSuchElementException(s"Path [$loc] in $Definitions is not of type object")
        }
        definition
      case None => inputJson
    }
  }

  private def toSparkStruct(properties: Seq[(String,JValue)], required: Seq[String]): StructType = {
    val sparkFields = properties.map {
      case (k, v) => toSparkStructField(getJsonName(v).getOrElse(k), v.extract[JObject], required.contains(k))
    }
    StructType(sparkFields)
  }

  private def toSparkStructField(name: String, tpe: JObject, required: Boolean): StructField = {
    val jsonResolved = resolveRefs(tpe)
    val fieldType = getFieldType(jsonResolved, name)
    StructField(getJsonName(jsonResolved).getOrElse(name), fieldType.dataType, fieldType.nullable || !required)
  }

  private def getFieldType(json: JObject, name: String): NullableDataType = {
    val fieldType = getJsonType(json, name)
    val sparkType = JsonToSparkTypeMap(fieldType.typeName)
    sparkType match {

      case dataType: DataType =>
        NullableDataType(dataType, fieldType.nullable)

      case ArrayType =>
        val innerJson = resolveRefs((json \ SchemaArrayItems).extract[JObject])
        val innerJsonType = getFieldType(innerJson, SchemaArrayItems)
        val dataType = ArrayType(innerJsonType.dataType, innerJsonType.nullable)
        NullableDataType(dataType, fieldType.nullable)

      // Struct type without properties but additionalProperties set, becomes a Spark MapType .
      case StructType if (json \ SchemaAdditionalProperties).toOption.nonEmpty && (json \ SchemaObjectProperties).toOption.isEmpty =>
        val dataType = (json \ SchemaAdditionalProperties) match {
          case JBool(true) => NullableDataType(StringType, nullable = true)
          case innerJson: JObject =>
            val innerJsonResolved = resolveRefs(innerJson)
            getFieldType(innerJsonResolved, SchemaAdditionalProperties)
          case x => throw new IllegalArgumentException(s"$SchemaAdditionalProperties cannot have value $x at <$name>")
        }
        NullableDataType(MapType(StringType, dataType.dataType), fieldType.nullable)

      case StructType =>
        val jsonResolved = resolveRefs(json)
        val properties = (jsonResolved \ SchemaObjectProperties).extractOpt[JObject].getOrElse(JObject())
        val required = (jsonResolved \ SchemaObjectPropertiesRequired).extractOpt[Seq[String]].getOrElse(Seq())
        val dataType = toSparkStruct(properties.obj, required)
        NullableDataType(dataType, fieldType.nullable)
    }
  }


}

case class SchemaType(typeName: String, nullable: Boolean)
private case class NullableDataType(dataType: DataType, nullable: Boolean)
