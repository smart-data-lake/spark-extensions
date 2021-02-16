package org.apache.spark.sql.custom

import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.avro.confluent.NullableHelper.makeNullable
import org.apache.spark.sql.avro.confluent.{CatalystDataToConfluentAvro, ConfluentAvroDataToCatalyst, ConfluentClient, MySchemaConverters}
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.functions.{lit, struct}
import org.mockito.Mockito._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.mockito.MockitoSugar.mock

class ConfluentAvroCatalystTest extends AnyFunSuite with Logging {

  // test data
  val row1 = Row(Row(true, "a"), 0f, "ok")
  val schemaId1 = 1
  val row2 = Row(Row(true, "a"), Row(true, "b"), 0f, "ok")
  val schemaId2 = 2
  val row1As2 = Row(Row(true, "a"), null, 0f, "ok")

  // expressions to define the dataType of the conversion
  val expr1 = struct(struct(lit(true).as("a1"), lit("testA").as("a2")).as("a")
    , lit(0f).as("c"), lit("ok").as("d")).expr
  val avroSchema1 = MySchemaConverters.toAvroType(expr1.dataType, expr1.nullable)
  val expr2 = struct(struct(lit(true).as("a1"), lit("testA").as("a2")).as("a")
                   , makeNullable(struct(lit(true).as("b1"), lit("testB").as("b2"))).as("b")
                   , lit(0f).as("c"), lit("ok").as("d")).expr
  val avroSchema2 = MySchemaConverters.toAvroType(expr2.dataType, expr2.nullable)

  // create internal rows
  val internalRowConverter1 = CatalystTypeConverters.createToCatalystConverter(expr1.dataType)
  val internalRow1 = internalRowConverter1(row1).asInstanceOf[InternalRow]
  val internalRowConverter2 = CatalystTypeConverters.createToCatalystConverter(expr2.dataType)
  val internalRow2 = internalRowConverter2(row2).asInstanceOf[InternalRow]
  val internalRow1As2 = internalRowConverter2(row1As2).asInstanceOf[InternalRow]

  // mock confluent client
  val confluentClientMock = mock[ConfluentClient]
  val subjectA = "testA-value"
  when(confluentClientMock.setOrGetSchema(subjectA, avroSchema1)).thenReturn((schemaId1, avroSchema1)) // write record1 with schema1 -> no conversion here
  when(confluentClientMock.setOrGetSchema(subjectA, avroSchema2)).thenReturn((schemaId2, avroSchema2))
  when(confluentClientMock.getSchemaFromConfluent(schemaId1)).thenReturn((schemaId1, avroSchema1))
  when(confluentClientMock.getSchemaFromConfluent(schemaId2)).thenReturn((schemaId2, avroSchema2))
  when(confluentClientMock.getLatestSchemaFromConfluent(subjectA)).thenReturn((schemaId2, avroSchema2)) // latest schema is schema2
  val subjectB = "testB-value"
  when(confluentClientMock.setOrGetSchema(subjectB, avroSchema1)).thenReturn((schemaId2, avroSchema2)) // write record1 with schema2

  test("convert row with nested type to avro and back") {

    // convert to avro
    val toAvroConverter = CatalystDataToConfluentAvro(expr2, subjectA, confluentClientMock, updateAllowed = false)
    val confluentAvroMsg = toAvroConverter.nullSafeEval(internalRow2)

    // convert back to spark row
    val toRowConverter = ConfluentAvroDataToCatalyst(expr2, subjectA, confluentClientMock)
    val finalInternalRow = toRowConverter.nullSafeEval(confluentAvroMsg)

    assert(internalRow2 == finalInternalRow)
  }

  test("schema evolution on read: convert row with old schema to avro and back to row with current schema") {

    // convert to avro
    val toAvroConverter = CatalystDataToConfluentAvro(expr1, subjectA, confluentClientMock, updateAllowed = false)
    val confluentAvroMsg = toAvroConverter.nullSafeEval(internalRow1)

    // convert back to spark row
    val toRowConverter = ConfluentAvroDataToCatalyst(expr2, subjectA, confluentClientMock)
    val finalInternalRow = toRowConverter.nullSafeEval(confluentAvroMsg)

    assert(internalRow1As2 == finalInternalRow)
  }

  // Doesn't work with current implementation of CatalystDataToConfluentAvro / MyAvroSerializer as they are based on field positions.
  ignore("schema evolution on write: convert row with old schema to avro with new schema and back to row") {

    // convert to avro with new schema
    val toAvroConverter = CatalystDataToConfluentAvro(expr1, subjectB, confluentClientMock, updateAllowed = false)
    val confluentAvroMsg = toAvroConverter.nullSafeEval(internalRow1)

    // convert back to spark row
    val toRowConverter = ConfluentAvroDataToCatalyst(expr2, subjectB, confluentClientMock)
    val finalInternalRow = toRowConverter.nullSafeEval(confluentAvroMsg)

    assert(internalRow1As2 == finalInternalRow)
  }

}
