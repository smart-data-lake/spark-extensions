package org.apache.spark.sql.confluent.avro

import io.confluent.kafka.schemaregistry.avro.AvroSchema
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.confluent.SubjectType
import org.apache.spark.sql.functions.struct
import org.mockito.Mockito._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.mockito.MockitoSugar.mock

class ConfluentAvroConnectorTest extends AnyFunSuite with Logging {

  private val spark = SparkSession.builder().master("local").getOrCreate()
  import spark.implicits._

  // test data
  private val data1 = Seq(((true, "a"), 0f, "ok"))
  private val df1 = data1.toDF("a","c","d")
  private val schemaId1 = 1

  // json schemas
  val jsonSchema1 = new AvroSchema(MySchemaConverters.toAvroType(df1.schema))

  // mock confluent client
  val confluentClientMock = mock[AvroConfluentClient]
  val topicA = "testA"
  val subjectA = s"$topicA-value"
  when(confluentClientMock.setOrGetSchema(subjectA, jsonSchema1)).thenReturn((schemaId1, jsonSchema1))
  when(confluentClientMock.getSchemaFromConfluent(schemaId1)).thenReturn((schemaId1, jsonSchema1))
  when(confluentClientMock.getLatestSchemaFromConfluent(subjectA)).thenReturn((schemaId1, jsonSchema1))
  when(confluentClientMock.getSubject(topicA,SubjectType.value)).thenReturn(subjectA)
  val avroConnector = new ConfluentAvroConnector(confluentClientMock)

  test("convert DataFrame with nested type to json and back") {

    // convert to avro
    val dfJson = df1
      .select(avroConnector.to_confluent_avro(struct("*"), topicA, SubjectType.value).as("avro"))

    // convert back to spark
    val dfSpark = dfJson
      .withColumn("data", avroConnector.from_confluent_avro($"avro", topicA, SubjectType.value).as("spark"))
      .select($"data.*")

    assert(df1.head == dfSpark.head)
  }

}
