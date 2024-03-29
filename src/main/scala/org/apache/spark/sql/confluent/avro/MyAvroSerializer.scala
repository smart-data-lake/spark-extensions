/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.confluent.avro

import org.apache.avro.Conversions.DecimalConversion
import org.apache.avro.LogicalTypes.{TimestampMicros, TimestampMillis}
import org.apache.avro.Schema.Type
import org.apache.avro.Schema.Type._
import org.apache.avro.generic.GenericData.{EnumSymbol, Fixed, Record}
import org.apache.avro.util.Utf8
import org.apache.avro.{LogicalTypes, Schema}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{SpecializedGetters, SpecificInternalRow}
import org.apache.spark.sql.confluent.IncompatibleSchemaException
import org.apache.spark.sql.types._

import java.nio.ByteBuffer
import scala.collection.JavaConverters._

/**
 * A serializer to serialize data in catalyst format to data in avro format.
 *
 * copied from org.apache.spark.sql.avro.*
 * Changes:
 * - backport support for nullable vs non-nullable types in resolveNullableType
 * - enhance resolveNullableType to support converting NullType to Null-Union
 * - enhance resolveNullableType to support converting not nullable type to Null-Union
 * - improve exception messages by adding field name
 */
class MyAvroSerializer(rootCatalystType: DataType, rootAvroType: Schema, nullable: Boolean) extends Logging {

  def serialize(catalystData: Any): Any = {
    converter.apply(catalystData)
  }

  private val converter: Any => Any = {
    val actualAvroType = resolveNullableType(rootAvroType, rootCatalystType, "root")
    val baseConverter = rootCatalystType match {
      case st: StructType =>
        newStructConverter(st, actualAvroType, "root").asInstanceOf[Any => Any]
      case _ =>
        val tmpRow = new SpecificInternalRow(Seq(rootCatalystType))
        val converter = newConverter(rootCatalystType, actualAvroType, "root")
        (data: Any) =>
          tmpRow.update(0, data)
          converter.apply(tmpRow, 0)
    }
    if (nullable) {
      (data: Any) =>
        if (data == null) {
          null
        } else {
          baseConverter.apply(data)
        }
    } else {
      baseConverter
    }
  }

  private type Converter = (SpecializedGetters, Int) => Any

  private lazy val decimalConversions = new DecimalConversion()

  private def newConverter(catalystType: DataType, avroType: Schema, fieldName: String): Converter = {
    (catalystType, avroType.getType) match {
      case (NullType, NULL) =>
        (getter, ordinal) => null
      case (BooleanType, BOOLEAN) =>
        (getter, ordinal) => getter.getBoolean(ordinal)
      case (ByteType, INT) =>
        (getter, ordinal) => getter.getByte(ordinal).toInt
      case (ShortType, INT) =>
        (getter, ordinal) => getter.getShort(ordinal).toInt
      case (IntegerType, INT) =>
        (getter, ordinal) => getter.getInt(ordinal)
      case (LongType, LONG) =>
        (getter, ordinal) => getter.getLong(ordinal)
      case (FloatType, FLOAT) =>
        (getter, ordinal) => getter.getFloat(ordinal)
      case (DoubleType, DOUBLE) =>
        (getter, ordinal) => getter.getDouble(ordinal)
      case (d: DecimalType, FIXED)
        if avroType.getLogicalType == LogicalTypes.decimal(d.precision, d.scale) =>
        (getter, ordinal) =>
          val decimal = getter.getDecimal(ordinal, d.precision, d.scale)
          decimalConversions.toFixed(decimal.toJavaBigDecimal, avroType,
            LogicalTypes.decimal(d.precision, d.scale))

      case (d: DecimalType, BYTES)
        if avroType.getLogicalType == LogicalTypes.decimal(d.precision, d.scale) =>
        (getter, ordinal) =>
          val decimal = getter.getDecimal(ordinal, d.precision, d.scale)
          decimalConversions.toBytes(decimal.toJavaBigDecimal, avroType,
            LogicalTypes.decimal(d.precision, d.scale))

      case (StringType, ENUM) =>
        val enumSymbols: Set[String] = avroType.getEnumSymbols.asScala.toSet
        (getter, ordinal) =>
          val data = getter.getUTF8String(ordinal).toString
          if (!enumSymbols.contains(data)) {
            throw new IncompatibleSchemaException(
              "Cannot write \"" + data + "\" since it's not defined in enum \"" +
                enumSymbols.mkString("\", \"") + "\"")
          }
          new EnumSymbol(avroType, data)

      case (StringType, STRING) =>
        (getter, ordinal) => new Utf8(getter.getUTF8String(ordinal).getBytes)

      case (BinaryType, FIXED) =>
        val size = avroType.getFixedSize
        (getter, ordinal) =>
          val data: Array[Byte] = getter.getBinary(ordinal)
          if (data.length != size) {
            throw new IncompatibleSchemaException(
              s"Cannot write ${data.length} ${if (data.length > 1) "bytes" else "byte"} of " +
                "binary data into FIXED Type with size of " +
                s"$size ${if (size > 1) "bytes" else "byte"}")
          }
          new Fixed(avroType, data)

      case (BinaryType, BYTES) =>
        (getter, ordinal) => ByteBuffer.wrap(getter.getBinary(ordinal))

      case (DateType, INT) =>
        (getter, ordinal) => getter.getInt(ordinal)

      case (TimestampType, LONG) => avroType.getLogicalType match {
        case _: TimestampMillis => (getter, ordinal) => getter.getLong(ordinal) / 1000
        case _: TimestampMicros => (getter, ordinal) => getter.getLong(ordinal)
        // For backward compatibility, if the Avro type is Long and it is not logical type,
        // output the timestamp value as with millisecond precision.
        case null => (getter, ordinal) => getter.getLong(ordinal) / 1000
        case other => throw new IncompatibleSchemaException(
          s"Cannot convert Catalyst Timestamp type to Avro logical type $other")
      }

      case (ArrayType(et, containsNull), ARRAY) =>
        val elementConverter = newConverter(
          et, resolveNullableType(avroType.getElementType, et, fieldName), fieldName)
        (getter, ordinal) => {
          val arrayData = getter.getArray(ordinal)
          val len = arrayData.numElements()
          val result = new Array[Any](len)
          var i = 0
          while (i < len) {
            if (containsNull && arrayData.isNullAt(i)) {
              result(i) = null
            } else {
              result(i) = elementConverter(arrayData, i)
            }
            i += 1
          }
          // avro writer is expecting a Java Collection, so we convert it into
          // `ArrayList` backed by the specified array without data copying.
          java.util.Arrays.asList(result: _*)
        }

      case (st: StructType, RECORD) =>
        val structConverter = newStructConverter(st, avroType, fieldName)
        val numFields = st.length
        (getter, ordinal) => structConverter(getter.getStruct(ordinal, numFields))

      case (MapType(kt, vt, valueContainsNull), MAP) if kt == StringType =>
        val valueConverter = newConverter(
          vt, resolveNullableType(avroType.getValueType, vt, fieldName), fieldName)
        (getter, ordinal) =>
          val mapData = getter.getMap(ordinal)
          val len = mapData.numElements()
          val result = new java.util.HashMap[String, Any](len)
          val keyArray = mapData.keyArray()
          val valueArray = mapData.valueArray()
          var i = 0
          while (i < len) {
            val key = keyArray.getUTF8String(i).toString
            if (valueContainsNull && valueArray.isNullAt(i)) {
              result.put(key, null)
            } else {
              result.put(key, valueConverter(valueArray, i))
            }
            i += 1
          }
          result

      case other =>
        throw new IncompatibleSchemaException(s"""($fieldName) Cannot convert Catalyst type $catalystType to """ +
          s"Avro type $avroType.")
    }
  }

  private def newStructConverter(catalystStruct: StructType, avroStruct: Schema, fieldName: String): InternalRow => Record = {
    if (avroStruct.getType != RECORD || avroStruct.getFields.size() != catalystStruct.length) {
      throw new IncompatibleSchemaException(s"""($fieldName) Cannot convert Catalyst type $catalystStruct to """ +
        s"Avro type $avroStruct.")
    }
    val fieldConverters = catalystStruct.zip(avroStruct.getFields.asScala).map {
      case (f1, f2) => newConverter(f1.dataType, resolveNullableType(f2.schema(), f1.dataType, f1.name), f1.name)
    }
    val numFields = catalystStruct.length
    (row: InternalRow) =>
      val result = new Record(avroStruct)
      var i = 0
      while (i < numFields) {
        if (row.isNullAt(i)) {
          result.put(i, null)
        } else {
          result.put(i, fieldConverters(i).apply(row, i))
        }
        i += 1
      }
      result
  }

  private def resolveNullableType(avroType: Schema, catalystType: DataType, fieldName: String): Schema = {
    if (avroType.getType == Type.UNION) {
      // avro uses union to represent nullable type.
      val fields = avroType.getTypes.asScala
      assert(fields.length == 2)
      val actualType = if (catalystType == NullType) fields.filter(_.getType == Type.NULL)
      else fields.filter(_.getType != Type.NULL)
      assert(actualType.length == 1)
      actualType.head
    } else {
      if (nullable) {
        logWarning(s"""($fieldName) Writing avro files with non-nullable avro schema with nullable catalyst """ +
          "schema will throw runtime exception if there is a record with null value.")
      }
      avroType
    }
  }
}
