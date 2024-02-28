package org.apache.spark.sql.confluent.json

import io.confluent.kafka.schemaregistry.json.JsonSchema
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.confluent.{ConfluentClient, SubjectType}
import org.apache.spark.sql.functions.struct
import org.json4s.jackson.JsonMethods.asJsonNode
import org.mockito.Mockito._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.mockito.MockitoSugar.mock

class ConfluentJsonConnectorTest extends AnyFunSuite with Logging {

  private val spark = SparkSession.builder().master("local").getOrCreate()
  import spark.implicits._

  // test data
  private val data1 = Seq(((true, "a"), BigDecimal(0), "ok"))
  private val df1 = data1.toDF("a","c","d")
  private val schemaId1 = 1

  // json schemas
  val jsonSchema1 = new JsonSchema(asJsonNode(SparkToJsonSchemaConverter.convert(df1.schema)))

  // mock confluent client
  val confluentClientMock = mock[ConfluentClient[JsonSchema]]
  val topicA = "testA"
  val subjectA = s"$topicA-value"
  when(confluentClientMock.setOrGetSchema(subjectA, jsonSchema1)).thenReturn((schemaId1, jsonSchema1))
  when(confluentClientMock.getSchemaFromConfluent(schemaId1)).thenReturn((schemaId1, jsonSchema1))
  when(confluentClientMock.getLatestSchemaFromConfluent(subjectA)).thenReturn((schemaId1, jsonSchema1))
  when(confluentClientMock.getSubject(topicA,SubjectType.value)).thenReturn(subjectA)
  val jsonConnector = new ConfluentJsonConnector(confluentClientMock)

  test("convert DataFrame with nested type to json and back") {

    // convert to avro
    val dfJson = df1
      .select(jsonConnector.to_confluent(struct("*"), topicA, SubjectType.value).as("json"))

    // convert back to spark
    val dfSpark = dfJson
      .withColumn("data", jsonConnector.from_confluent($"json", topicA, SubjectType.value).as("spark"))
      .select($"data.*")

    assert(df1.head == dfSpark.head)
  }

}
