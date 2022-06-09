package org.apache.spark.sql.confluent.json

import org.apache.spark.sql.types._
import org.json4s.{DefaultFormats, Formats, JObject}

object JsonSchemaConverter {

  private[json] val SchemaFieldName = "name"
  private[json] val SchemaFieldType = "type"
  private[json] val SchemaFieldId = "id"
  private[json] val SchemaObjectProperties = "properties"
  private[json] val SchemaObjectPropertiesRequired = "required"
  private[json] val SchemaAdditionalProperties = "additionalProperties"
  private[json] val SchemaArrayItems = "items"
  private[json] val SchemaRoot = "/"
  private[json] val Definitions = "definitions"
  private[json] val Reference = "$ref"
  private[json] val JsonToSparkTypeMap = Map[String, AbstractDataType](
    "string" -> StringType,
    "number" -> DoubleType,
    "integer" -> LongType,
    "boolean" -> BooleanType,
    "date-time" -> TimestampType,
    "object" -> StructType,
    "array" -> ArrayType,
  )
  private[json] val SparkToJsonTypeMap = Map[AbstractDataType, String](
    IntegerType -> "integer",
    ShortType -> "integer",
    ByteType -> "integer",
    DateType -> "date-time",
    StringType -> "string",
    DoubleType -> "number",
    FloatType -> "number",
    LongType -> "integer",
    BooleanType-> "boolean",
    TimestampType -> "date-time",
    StructType -> "object",
    ArrayType -> "array"
    // DecimalType is handled specifically in code because the mapping is not 1:1.
  )

  def convertToSpark(schemaContent: String, isStrictTypingEnabled: Boolean): StructType = {
    import org.json4s.jackson.JsonMethods._
    implicit val format: Formats = DefaultFormats
    val schema = parse(schemaContent).extract[JObject]
    convertToSpark(schema, isStrictTypingEnabled)
  }

  def convertToSpark(schema: JObject, isStrictTypingEnabled: Boolean): StructType = {
    new JsonToSparkSchemaConverter(schema, isStrictTypingEnabled).convert()
  }

  def convertFromSpark(schema: StructType): JObject = {
    SparkToJsonSchemaConverter.convert(schema)
  }

  def convertFromSparkToString(schema: StructType): String = {
    import org.json4s.jackson.JsonMethods._
    implicit val format: Formats = DefaultFormats
    val json = convertFromSpark(schema)
    pretty(json)
  }
}
