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

package org.apache.spark.sql.avro.confluent

import java.nio.ByteBuffer

import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.io.{BinaryDecoder, DecoderFactory}
import org.apache.spark.sql.avro.AvroDeserializer
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode}
import org.apache.spark.sql.types.{AbstractDataType, BinaryType, DataType}

import scala.collection.mutable

// copied from org.apache.spark.sql.avro.*
case class ConfluentAvroDataToCatalyst(child: Expression, subject: String, confluentHelper: ConfluentClient)
  extends UnaryExpression with ExpectsInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(BinaryType)

  override lazy val dataType: DataType = tgtDataType

  override def nullable: Boolean = true

  // prepare reader and deserializer for avro schema
  @transient private lazy val (tgtDataType, tgtSchemaId, tgtReader, tgtDeserializer) = {
    // Avro schema is not serializable. We must be careful to not store it in an attribute of the class.
    val (schemaId, schema) = confluentHelper.getLatestSchemaFromConfluent(subject)
    val dataType = MySchemaConverters.toSqlType(schema).dataType
    val reader = new GenericDatumReader[Any](schema)
    val deserializer = new AvroDeserializer(schema, dataType)
    (dataType, schemaId, reader, deserializer)
  }
  // To decode a message we need to use the schema referenced by the message. Therefore we might need different deserializers.
  @transient private lazy val deserializers = mutable.Map(tgtSchemaId -> tgtDeserializer)

  @transient private var decoder: BinaryDecoder = _

  @transient private var result: Any = _

  override def nullSafeEval(input: Any): Any = {
    val binary = input.asInstanceOf[Array[Byte]]
    val (schemaId,avroMsg) = parseConfluentMsg(binary)
    val (_,msgSchema) = confluentHelper.getSchemaFromConfluent(schemaId)
    decoder = DecoderFactory.get().binaryDecoder(avroMsg, 0, avroMsg.length, decoder)
    result = tgtReader.read(result, decoder)
    deserializers.getOrElseUpdate(schemaId, new AvroDeserializer(msgSchema, dataType))
      .deserialize(result)
  }

  override def prettyName: String = "from_avro"

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val expr = ctx.addReferenceObj("this", this)
    defineCodeGen(ctx, ev, input =>
      s"(${CodeGenerator.boxedType(dataType)})$expr.nullSafeEval($input)")
  }

  def parseConfluentMsg(msg:Array[Byte]): (Int,Array[Byte]) = {
    val msgBuffer = ByteBuffer.wrap(msg)
    val magicByte = msgBuffer.get
    require(magicByte == confluentHelper.CONFLUENT_MAGIC_BYTE, "Magic byte not present at start of confluent message!")
    val schemaId = msgBuffer.getInt
    val avroMsg = msg.slice(msgBuffer.position, msgBuffer.limit)
    //return
    (schemaId, avroMsg)
  }
}
