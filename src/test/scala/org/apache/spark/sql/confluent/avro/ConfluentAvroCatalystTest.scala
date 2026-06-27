package org.apache.spark.sql.confluent.avro

import io.confluent.kafka.schemaregistry.avro.AvroSchema
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.avro.{AvroOptions, SchemaConverters}
import org.apache.spark.sql.catalyst.expressions.BoundReference
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.confluent.{ConfluentClient, avro}
import org.apache.spark.sql.types._
import org.mockito.Mockito._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.mockito.MockitoSugar.mock

class ConfluentAvroCatalystTest extends AnyFunSuite with Logging {

  // test data
  private val row1 = Row(Row(true, "a"), 0f, "ok", 1)
  private val schemaId1 = 1
  private val row2 = Row(Row(true, "a"), Row(true, "b"), 0f, "ok", 1L)
  private val schemaId2 = 2
  private val row1As2 = Row(Row(true, "a"), null, 0f, "ok", 1L)

  // resolved schemas / expressions used to define the conversion types
  private val schema1 = StructType(Seq(
    StructField("a", StructType(Seq(
      StructField("a1", BooleanType, nullable = false),
      StructField("a2", StringType, nullable = false)
    )), nullable = false),
    StructField("c", FloatType, nullable = false),
    StructField("d", StringType, nullable = false),
    StructField("e", IntegerType, nullable = false)
  ))
  private val expr1 = BoundReference(0, schema1, nullable = false)
  private val avroSchema1 = new AvroSchema(SchemaConverters.toAvroType(schema1, nullable = false))

  private val schema2 = StructType(Seq(
    StructField("a", StructType(Seq(
      StructField("a1", BooleanType, nullable = false),
      StructField("a2", StringType, nullable = false)
    )), nullable = false),
    StructField("b", StructType(Seq(
      StructField("b1", BooleanType, nullable = false),
      StructField("b2", StringType, nullable = false)
    )), nullable = true),
    StructField("c", FloatType, nullable = false),
    StructField("d", StringType, nullable = false),
    StructField("e", LongType, nullable = false)
  ))
  private val expr2 = BoundReference(0, schema2, nullable = false)
  private val avroSchema2 = new AvroSchema(SchemaConverters.toAvroType(schema2, nullable = false))

  // create internal rows
  private val internalRowConverter1 = CatalystTypeConverters.createToCatalystConverter(schema1)
  private val internalRow1 = internalRowConverter1(row1).asInstanceOf[InternalRow]
  private val internalRowConverter2 = CatalystTypeConverters.createToCatalystConverter(schema2)
  private val internalRow2 = internalRowConverter2(row2).asInstanceOf[InternalRow]
  private val internalRow1As2 = internalRowConverter2(row1As2).asInstanceOf[InternalRow]

  // mock confluent client
  private val confluentClientMock = mock[ConfluentClient[AvroSchema]]
  private val subjectA = "testA-value"
  when(confluentClientMock.setOrGetSchema(subjectA, avroSchema1)).thenReturn((schemaId1, avroSchema1)) // write record1 with schema1 -> no conversion here
  when(confluentClientMock.setOrGetSchema(subjectA, avroSchema2)).thenReturn((schemaId2, avroSchema2))
  when(confluentClientMock.getSchemaFromConfluent(schemaId1)).thenReturn((schemaId1, avroSchema1))
  when(confluentClientMock.getSchemaFromConfluent(schemaId2)).thenReturn((schemaId2, avroSchema2))
  when(confluentClientMock.getLatestSchemaFromConfluent(subjectA)).thenReturn((schemaId2, avroSchema2)) // latest schema is schema2
  private val subjectB = "testB-value"
  when(confluentClientMock.setOrGetSchema(subjectB, avroSchema1)).thenReturn((schemaId2, avroSchema2)) // write record1 with schema2

  test("convert row with nested type to avro and back") {

    // convert to avro
    val toAvroConverter = CatalystDataToConfluentAvro(expr2, subjectA, confluentClientMock, updateAllowed = false)
    val confluentAvroMsg = toAvroConverter.nullSafeEval(internalRow2)

    // convert back to spark row
    val toRowConverter = ConfluentAvroDataToCatalyst(expr2, subjectA, confluentClientMock, AvroOptions(Map()))
    val finalInternalRow = toRowConverter.nullSafeEval(confluentAvroMsg)

    assert(finalInternalRow == internalRow2)
  }

  test("schema evolution on read: convert row with old schema to avro and back to row with current schema") {

    // convert to avro
    val toAvroConverter = avro.CatalystDataToConfluentAvro(expr1, subjectA, confluentClientMock, updateAllowed = false)
    val confluentAvroMsg = toAvroConverter.nullSafeEval(internalRow1)

    // convert back to spark row
    val toRowConverter = avro.ConfluentAvroDataToCatalyst(expr2, subjectA, confluentClientMock, AvroOptions(Map()))
    val finalInternalRow = toRowConverter.nullSafeEval(confluentAvroMsg)

    assert(internalRow1As2 == finalInternalRow)
  }

  // Doesn't work with current implementation of CatalystDataToConfluentAvro / AvroSerializer as it does not yet support type widening from Int to Long for field e.
  ignore("schema evolution on write: convert row with old schema to avro with new schema and back to row") {

    // convert to avro with new schema
    val toAvroConverter = avro.CatalystDataToConfluentAvro(expr1, subjectB, confluentClientMock, updateAllowed = false)
    val confluentAvroMsg = toAvroConverter.nullSafeEval(internalRow1)

    // convert back to spark row
    val toRowConverter = avro.ConfluentAvroDataToCatalyst(expr2, subjectB, confluentClientMock, AvroOptions(Map()))
    val finalInternalRow = toRowConverter.nullSafeEval(confluentAvroMsg)

    assert(internalRow1As2 == finalInternalRow)
  }

}
