package org.apache.spark.sql.confluent.json

import org.apache.spark.sql.types._
import org.json4s._


case class SchemaType(dataType: DataType, nullable: Boolean)

object JsonSchemaConverter {

  private[json] val SchemaFieldName = "name"
  private[json] val SchemaFieldType = "type"
  private[json] val SchemaFieldFormat = "format"
  private[json] val SchemaFieldAirbyteType = "airbyte_type"
  private[json] val SchemaFieldOneOf = "oneOf"
  private[json] val SchemaFieldId = "id"
  private[json] val SchemaFieldProperties = "properties"
  private[json] val SchemaFieldItems = "items"
  private[json] val SchemaFieldAdditionalProperties = "additionalProperties"
  private[json] val SchemaFieldDescription = "description"
  private[json] val SchemaFieldRequired = "required"
  private[json] val SchemaRoot = "/"
  private[json] val Definitions = "definitions"
  private[json] val Reference = "$ref"
  private[json] val JsonToSparkTypeMap = Map[String,DataType](
    "string" -> StringType,
    "number" -> DecimalType.SYSTEM_DEFAULT,
    "float" -> DoubleType,
    "integer" -> LongType,
    "boolean" -> BooleanType,
    "date-time" -> TimestampType,
    "date-time-ntz" -> TimestampNTZType,
    "date" -> DateType
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

  def convertToSpark(schemaContent: String, isStrictTypingEnabled: Boolean = true, additionalPropertiesDefault: Boolean = true): StructType = {
    import org.json4s.jackson.JsonMethods._
    implicit val format: Formats = DefaultFormats
    val schema = parse(schemaContent).extract[JObject]
    convertParsedSchemaToSpark(schema, isStrictTypingEnabled, additionalPropertiesDefault)
  }

  def convertParsedSchemaToSpark(schema: JObject, isStrictTypingEnabled: Boolean = true, additionalPropertiesDefault: Boolean = true): StructType = {
    new JsonToSparkSchemaConverter(schema, isStrictTypingEnabled, additionalPropertiesDefault).convert()
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
