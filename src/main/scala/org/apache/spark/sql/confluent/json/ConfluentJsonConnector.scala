package org.apache.spark.sql.confluent.json

import io.confluent.kafka.schemaregistry.json.JsonSchema
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, ExprUtils, Expression, NullIntolerant, StructsToJson, TimeZoneAwareExpression, UnaryExpression}
import org.apache.spark.sql.catalyst.json.{JSONOptions, JacksonGenerator, JacksonUtils}
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.confluent.ConfluentClient
import org.apache.spark.sql.confluent.SubjectType.SubjectType
import org.apache.spark.sql.types.{AbstractDataType, ArrayType, DataType, MapType, StringType, StructType, TypeCollection}
import org.apache.spark.sql.{Column, functions}
import org.apache.spark.unsafe.types.UTF8String
import org.json4s.JsonAST.JObject

import java.io.CharArrayWriter

/**
 * Provides Spark SQL functions from/to_confluent_avro for decoding/encoding confluent avro messages.
 */
class ConfluentJsonConnector(confluentClient: ConfluentClient[JsonSchema]) extends Serializable {

  /**
   * Converts a binary column of confluent avro format into its corresponding catalyst value according to the latest
   * schema stored in schema registry.
   * Note: copied from org.apache.spark.sql.avro.*
   *
   * @param data the binary column.
   * @param topic the topic name.
   * @param subjectType the subject type (key or value).
   */
  def from_confluent_json(data: Column, topic: String, subjectType: SubjectType): Column = {
    import org.json4s.jackson.JsonMethods.fromJsonNode
    val subject = confluentClient.getSubject(topic, subjectType)
    val (schemaId, schema) = confluentClient.getLatestSchemaFromConfluent(subject)
    val schemaJson = fromJsonNode(schema.toJsonNode).asInstanceOf[JObject]
    val sparkSchema = JsonSchemaConverter.convertToSpark(schemaJson, false)
    functions.from_json(data, sparkSchema)
  }

  /**
   * Converts a column into binary of confluent avro format according to the latest schema stored in schema registry.
   * Note: copied from org.apache.spark.sql.avro.*
   *
   * @param data the data column.
   * @param topic the topic name.
   * @param subjectType the subject type (key or value).
   * @param updateAllowed if subject schema should be updated if compatible
   * @param mutualReadCheck if a mutual read check or a simpler can read check should be executed
   */
  def to_confluent_json(data: Column, topic: String, subjectType: SubjectType, updateAllowed: Boolean = false, mutualReadCheck: Boolean = false): Column = {
    to_json_confluent(data, confluentClient, topic, subjectType, updateAllowed, mutualReadCheck)
  }

  /**
   * copied from spark.sql.functions.to_json to customize StructsToJsonWithConfluent
   */
  private def to_json_confluent(e: Column, confluentClient: ConfluentClient[JsonSchema],
                                topic: String, subjectType: SubjectType, updateAllowed: Boolean = false, mutualReadCheck: Boolean = false, options: Map[String, String] = Map()): Column = {
    Column(StructsToJsonWithConfluent(options, e.expr, confluentClient, topic, subjectType, updateAllowed, mutualReadCheck))
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
                          topic: String, subjectType: SubjectType, updateAllowed: Boolean = false, mutualReadCheck: Boolean = false,
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
    val schema = child.dataType.asInstanceOf[StructType]
    import org.json4s.jackson.JsonMethods.asJsonNode
    val subject = confluentClient.getSubject(topic, subjectType)
    val jsonSchema = new JsonSchema(asJsonNode(JsonSchemaConverter.convertFromSpark(schema)))
    if (updateAllowed) confluentClient.setOrUpdateSchema(subject, jsonSchema, mutualReadCheck)
    else confluentClient.setOrGetSchema(subject, jsonSchema)
    schema
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
        JacksonUtils.verifySchema(struct)
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

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression =
    copy(timeZoneId = Option(timeZoneId))

  override def nullSafeEval(value: Any): Any = converter(value)

  override def inputTypes: Seq[AbstractDataType] = TypeCollection(ArrayType, StructType) :: Nil

  override def prettyName: String = "to_json"
}
