package org.apache.spark.sql.confluent.json

import io.confluent.kafka.schemaregistry.json.JsonSchema
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.DataTypeMismatch
import org.apache.spark.sql.catalyst.dsl.expressions.DslExpression
import org.apache.spark.sql.catalyst.expressions.json.StructsToJsonEvaluator
import org.apache.spark.sql.catalyst.expressions.objects.Invoke
import org.apache.spark.sql.catalyst.expressions.{DefaultStringProducingExpression, ExpectsInputTypes, Expression, Literal, RuntimeReplaceable, TimeZoneAwareExpression, UnaryExpression}
import org.apache.spark.sql.catalyst.json.JacksonUtils
import org.apache.spark.sql.catalyst.trees.TreePattern.{RUNTIME_REPLACEABLE, TreePattern}
import org.apache.spark.sql.confluent.SubjectType.SubjectType
import org.apache.spark.sql.confluent.{ConfluentClient, ConfluentConnector, IncompatibleSchemaException}
import org.apache.spark.sql.errors.QueryErrorsBase
import org.apache.spark.sql.types._
import org.json4s.JsonAST.JObject

/**
 * Provides Spark SQL functions from/to_confluent for decoding/encoding confluent json messages.
 */
class ConfluentJsonConnector(confluentClient: ConfluentClient[JsonSchema]) extends ConfluentConnector {

  /**
   * Converts a binary column of confluent json format into its corresponding catalyst value
   * according to the latest schema stored in schema registry.
   * @param data
   *   the binary column.
   * @param topic
   *   the topic name.
   * @param subjectType
   *   the subject type (key or value).
   */
  override def from_confluent(data: Expression, topic: String, subjectType: SubjectType, options: Map[String, String]): Expression = {
    import org.json4s.jackson.JsonMethods.fromJsonNode
    val subject = confluentClient.getSubject(topic, subjectType)
    val (schemaId, schema) = confluentClient.getLatestSchemaFromConfluent(subject)
    val schemaJson = fromJsonNode(schema.toJsonNode).asInstanceOf[JObject]
    val sparkSchema = JsonSchemaConverter.convertParsedSchemaToSpark(schemaJson, isStrictTypingEnabled = false)
    import org.apache.spark.sql.catalyst.expressions.JsonToStructs
    JsonToStructs(sparkSchema, options, data.cast(StringType))
  }

  /**
   * Converts a column into binary of confluent json format according to the latest schema stored in
   * schema registry.
   * @param data
   *   the data column.
   * @param topic
   *   the topic name.
   * @param subjectType
   *   the subject type (key or value).
   * @param updateAllowed
   *   if subject schema should be updated if compatible
   * @param mutualReadCheck
   *   if a mutual read check or a simpler can read check should be executed
   * @param eagerCheck
   *   if true tiggers instantiation of converter object instances
   */
  override def to_confluent(
      data: Expression,
      topic: String,
      subjectType: SubjectType,
      updateAllowed: Boolean = false,
      mutualReadCheck: Boolean = false,
      eagerCheck: Boolean = false
  ): Expression =
    to_json_confluent(data, confluentClient, topic, subjectType, updateAllowed, mutualReadCheck, eagerCheck = eagerCheck)

  /**
   * copied from spark.sql.functions.to_json to customize StructsToJsonWithConfluent
   */
  private def to_json_confluent(
      e: Expression,
      confluentClient: ConfluentClient[JsonSchema],
      topic: String,
      subjectType: SubjectType,
      updateAllowed: Boolean = false,
      mutualReadCheck: Boolean = false,
      eagerCheck: Boolean = false,
      options: Map[String, String] = Map()
  ): Expression =
    StructsToJsonWithConfluent(options, e, confluentClient, topic, subjectType, updateAllowed, mutualReadCheck, eagerCheck)

}

object ConfluentJsonConnector {
  def apply(schemaRegistryUrl: String): ConfluentJsonConnector =
    new ConfluentJsonConnector(new ConfluentClient[JsonSchema](schemaRegistryUrl))
}

/**
 * As schema from an expression can only be retrieved at execution time, we need customize Sparks
 * StructsToJsonWithConfluent operator. This is copied from
 * spark.sql.catalyst.expressions.jsonExpressions.scala.
 */
case class StructsToJsonWithConfluent(
    options: Map[String, String],
    child: Expression,
    confluentClient: ConfluentClient[JsonSchema],
    topic: String,
    subjectType: SubjectType,
    updateAllowed: Boolean = false,
    mutualReadCheck: Boolean = false,
    eagerCheck: Boolean = false,
    timeZoneId: Option[String] = None,
) extends UnaryExpression
  with RuntimeReplaceable
  with ExpectsInputTypes
  with TimeZoneAwareExpression
  with DefaultStringProducingExpression
  with QueryErrorsBase {

  override def nullable: Boolean = true

  override def nodePatternsInternal(): Seq[TreePattern] = Seq(RUNTIME_REPLACEABLE)

  @transient
  lazy val inputSchema: DataType = {
    // CHANGED: create/update schema in confluent
    val newSchema = child.dataType.asInstanceOf[StructType]
    import org.json4s.jackson.JsonMethods.asJsonNode
    val subject = confluentClient.getSubject(topic, subjectType)
    val newJsonSchema = new JsonSchema(asJsonNode(JsonSchemaConverter.convertFromSpark(newSchema)))
    val (schemaId, jsonSchema) = if (updateAllowed) confluentClient.setOrUpdateSchema(subject, newJsonSchema, mutualReadCheck)
    else confluentClient.setOrGetSchema(subject, newJsonSchema)
    if (!updateAllowed && newJsonSchema != jsonSchema) throw new IncompatibleSchemaException(
      s"New schema for subject $subject is different from existing schema and updateAllowed=false: Existing=$jsonSchema New=$newJsonSchema"
    )
    newSchema
  }


  override def checkInputDataTypes(): TypeCheckResult = inputSchema match {
    case dt @ (_: StructType | _: MapType | _: ArrayType | _: VariantType) =>
      JacksonUtils.verifyType(prettyName, dt)
    case _ =>
      DataTypeMismatch(
        errorSubClass = "INVALID_JSON_SCHEMA",
        messageParameters = Map("schema" -> toSQLType(child.dataType)))
  }

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression =
    copy(timeZoneId = Option(timeZoneId))

  override def inputTypes: Seq[AbstractDataType] = TypeCollection(ArrayType, StructType) :: Nil

  override def prettyName: String = "to_json"

  override protected def withNewChildInternal(newChild: Expression): StructsToJsonWithConfluent =
    copy(child = newChild)

  @transient
  private lazy val evaluator = StructsToJsonEvaluator(options, inputSchema, timeZoneId)

  override def replacement: Expression = Invoke(
    Literal.create(evaluator, ObjectType(classOf[StructsToJsonEvaluator])),
    "evaluate",
    dataType,
    Seq(child),
    Seq(child.dataType)
  )
}
