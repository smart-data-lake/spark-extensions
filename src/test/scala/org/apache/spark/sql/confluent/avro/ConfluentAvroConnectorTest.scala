package org.apache.spark.sql.confluent.avro

import io.confluent.kafka.schemaregistry.avro.AvroSchema
import org.apache.spark.internal.Logging
import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.classic.{ColumnConversions, ColumnNodeToExpressionConverter, ExpressionUtils}
import org.apache.spark.sql.confluent.SubjectType
import org.apache.spark.sql.functions.struct
import org.apache.spark.sql.{Column, SparkSession}
import org.mockito.Mockito._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.mockito.MockitoSugar.mock

import scala.language.implicitConversions

class ConfluentAvroConnectorTest extends AnyFunSuite with Logging with ColumnConversions {

  override protected def converter: ColumnNodeToExpressionConverter = ColumnNodeToExpressionConverter
  private implicit def toCol(e: Expression): Column = ExpressionUtils.column(e)

  private val spark = SparkSession.builder().master("local").getOrCreate()
  import spark.implicits._

  // test data
  private val data1 = Seq(((true, "a"), 0f, "ok"))
  private val df1 = data1.toDF("a","c","d")
  private val schemaId1 = 1

  // schemas
  val schema1 = new AvroSchema(AvroHelper.fixNullableDefault(SchemaConverters.toAvroType(df1.schema)))

  // mock confluent client
  val confluentClientMock = mock[AvroConfluentClient]
  val topicA = "testA"
  val subjectA = s"$topicA-value"
  when(confluentClientMock.setOrGetSchema(subjectA, schema1)).thenReturn((schemaId1, schema1))
  when(confluentClientMock.getSchemaFromConfluent(schemaId1)).thenReturn((schemaId1, schema1))
  when(confluentClientMock.getLatestSchemaFromConfluent(subjectA)).thenReturn((schemaId1, schema1))
  when(confluentClientMock.getSubject(topicA,SubjectType.value)).thenReturn(subjectA)
  val avroConnector = new ConfluentAvroConnector(confluentClientMock)

  test("convert DataFrame with nested type to json and back") {

    // convert to avro
    val dfJson = df1
      .select(avroConnector.to_confluent(struct("*").expr, topicA, SubjectType.value).as("avro"))

    // convert back to spark
    val dfSpark = dfJson
      .withColumn("data", avroConnector.from_confluent($"avro".expr, topicA, SubjectType.value).as("spark"))
      .select($"data.*")

    assert(df1.head == dfSpark.head)
  }

}
