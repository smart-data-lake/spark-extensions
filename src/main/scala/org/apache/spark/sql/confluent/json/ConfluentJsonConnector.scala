package org.apache.spark.sql.confluent.json

import io.confluent.kafka.schemaregistry.json.JsonSchema
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, NullIntolerant, TimeZoneAwareExpression, UnaryExpression}
import org.apache.spark.sql.catalyst.json.{JSONOptions, JacksonGenerator, JacksonUtils}
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.confluent.SubjectType.SubjectType
import org.apache.spark.sql.confluent.{ConfluentClient, ConfluentConnector, IncompatibleSchemaException}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, functions}
import org.apache.spark.unsafe.types.UTF8String
import org.json4s.JsonAST.JObject

import java.io.CharArrayWriter

/**
 * Provides Spark SQL functions from/to_confluent for decoding/encoding confluent json messages.
 */
class ConfluentJsonConnector(confluentClient: ConfluentClient[JsonSchema]) extends ConfluentConnector {

  /**
   * Converts a binary column of confluent json format into its corresponding catalyst value according to the latest
   * schema stored in schema registry.
   * @param data the binary column.
   * @param topic the topic name.
   * @param subjectType the subject type (key or value).
   */
  override def from_confluent(data: Column, topic: String, subjectType: SubjectType): Column = {
    import org.json4s.jackson.JsonMethods.fromJsonNode
    val subject = confluentClient.getSubject(topic, subjectType)
    val (schemaId, schema) = confluentClient.getLatestSchemaFromConfluent(subject)
    val schemaJson = fromJsonNode(schema.toJsonNode).asInstanceOf[JObject]
    val sparkSchema = JsonSchemaConverter.convertToSpark(schemaJson, isStrictTypingEnabled = false)
    functions.from_json(data.cast(StringType), sparkSchema)
  }

  /**
   * Converts a column into binary of confluent json format according to the latest schema stored in schema registry.
   * @param data the data column.
   * @param topic the topic name.
   * @param subjectType the subject type (key or value).
   * @param updateAllowed if subject schema should be updated if compatible
   * @param mutualReadCheck if a mutual read check or a simpler can read check should be executed
   * @param eagerCheck if true tiggers instantiation of converter object instances
   */
  override def to_confluent(data: Column, topic: String, subjectType: SubjectType, updateAllowed: Boolean = false, mutualReadCheck: Boolean = false, eagerCheck: Boolean = false): Column = {
    to_json_confluent(data, confluentClient, topic, subjectType, updateAllowed, mutualReadCheck, eagerCheck = eagerCheck)
  }

  /**
   * copied from spark.sql.functions.to_json to customize StructsToJsonWithConfluent
   */
  private def to_json_confluent(e: Column, confluentClient: ConfluentClient[JsonSchema],
                                topic: String, subjectType: SubjectType, updateAllowed: Boolean = false, mutualReadCheck: Boolean = false, eagerCheck: Boolean = false, options: Map[String, String] = Map()): Column = {
    Column(StructsToJsonWithConfluent(options, e.expr, confluentClient, topic, subjectType, updateAllowed, mutualReadCheck, eagerCheck))
  }

}

object ConfluentJsonConnector {
  def apply(schemaRegistryUrl: String): ConfluentJsonConnector = {
    new ConfluentJsonConnector(new ConfluentClient[JsonSchema](schemaRegistryUrl))
  }
}

/**
 * As schema from an expression can only be retrieved at execution time, we need customize Sparks StructsToJsonWithConfluent operator.
 * This is copied from spark.sql.catalyst.expressions.jsonExpressions.scala.
 */
case class StructsToJsonWithConfluent(
                          options: Map[String, String],
                          child: Expression,
                          confluentClient: ConfluentClient[JsonSchema],
                          topic: String, subjectType: SubjectType, updateAllowed: Boolean = false, mutualReadCheck: Boolean = false, eagerCheck: Boolean = false,
                          timeZoneId: Option[String] = None)
  extends UnaryExpression with TimeZoneAwareExpression with CodegenFallback
    with ExpectsInputTypes with NullIntolerant {
  override def nullable: Boolean = true

  @transient
  lazy val writer = new CharArrayWriter()

  @transient
  lazy val gen = new JacksonGenerator(
    inputSchema, writer, new JSONOptions(options, timeZoneId.get))

  @transient
  lazy val inputSchema: DataType = {
    // CHANGED: create/update schema in confluent
    val newSchema = child.dataType.asInstanceOf[StructType]
    import org.json4s.jackson.JsonMethods.asJsonNode
    val subject = confluentClient.getSubject(topic, subjectType)
    val newJsonSchema = new JsonSchema(asJsonNode(JsonSchemaConverter.convertFromSpark(newSchema)))
    val (schemaId, jsonSchema) = if (updateAllowed) confluentClient.setOrUpdateSchema(subject, newJsonSchema, mutualReadCheck)
    else confluentClient.setOrGetSchema(subject, newJsonSchema)
    if (!updateAllowed && newJsonSchema != jsonSchema) throw new IncompatibleSchemaException(s"New schema for subject $subject is different from existing schema and updateAllowed=false: Existing=$jsonSchema New=$newJsonSchema")
    newSchema
  }

  // This converts rows to the JSON output according to the given schema.
  @transient
  lazy val converter: Any => UTF8String = {
    def getAndReset(): UTF8String = {
      gen.flush()
      val json = writer.toString
      writer.reset()
      UTF8String.fromString(json)
    }

    inputSchema match {
      case _: StructType =>
        (row: Any) =>
          gen.write(row.asInstanceOf[InternalRow])
          getAndReset()
      case _: ArrayType =>
        (arr: Any) =>
          gen.write(arr.asInstanceOf[ArrayData])
          getAndReset()
      case _: MapType =>
        (map: Any) =>
          gen.write(map.asInstanceOf[MapData])
          getAndReset()
    }
  }

  override def dataType: DataType = StringType

  override def checkInputDataTypes(): TypeCheckResult = inputSchema match {
    case struct: StructType =>
      try {
        struct.foreach(field => JacksonUtils.verifyType(field.name, field.dataType))
        TypeCheckResult.TypeCheckSuccess
      } catch {
        case e: UnsupportedOperationException =>
          TypeCheckResult.TypeCheckFailure(e.getMessage)
      }
    case map: MapType =>
      try {
        JacksonUtils.verifyType(prettyName, map)
        TypeCheckResult.TypeCheckSuccess
      } catch {
        case e: UnsupportedOperationException =>
          TypeCheckResult.TypeCheckFailure(e.getMessage)
      }
    case array: ArrayType =>
      try {
        JacksonUtils.verifyType(prettyName, array)
        TypeCheckResult.TypeCheckSuccess
      } catch {
        case e: UnsupportedOperationException =>
          TypeCheckResult.TypeCheckFailure(e.getMessage)
      }
    case _ => TypeCheckResult.TypeCheckFailure(
      s"Input type ${child.dataType.catalogString} must be a struct, array of structs or " +
        "a map or array of map.")
  }

  // trigger schema checks if eager validation is requested.
  if (eagerCheck) checkInputDataTypes()

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression =
    copy(timeZoneId = Option(timeZoneId))

  override def nullSafeEval(value: Any): Any = converter(value)

  override def inputTypes: Seq[AbstractDataType] = TypeCollection(ArrayType, StructType) :: Nil

  override def prettyName: String = "to_json"

  override protected def withNewChildInternal(newChild: Expression): StructsToJsonWithConfluent =
    copy(child = newChild)
}
