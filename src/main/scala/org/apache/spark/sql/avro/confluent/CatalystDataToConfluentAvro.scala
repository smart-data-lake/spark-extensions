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

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer

import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.io.{BinaryEncoder, EncoderFactory}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Expression, UnaryExpression}
import org.apache.spark.sql.types.{BinaryType, DataType}

// copied from org.apache.spark.sql.avro.*
case class CatalystDataToConfluentAvro(child: Expression, subject: String, confluentHelper: ConfluentClient, updateAllowed: Boolean, mutualReadCheck: Boolean = false) extends UnaryExpression with Logging {
  if (!updateAllowed && mutualReadCheck) logWarning("mutualReadCheck is ignored if updateAllowed=false")

  override def dataType: DataType = BinaryType

  @transient private lazy val newSchema = MySchemaConverters.toAvroType(child.dataType, child.nullable)

  @transient private lazy val (id,targetSchema) = if (updateAllowed) confluentHelper.setOrUpdateSchema(subject, newSchema, mutualReadCheck)
  else confluentHelper.setOrGetSchema(subject, newSchema)

  @transient private lazy val serializer = new MyAvroSerializer(child.dataType, targetSchema, child.nullable)

  @transient private lazy val writer = new GenericDatumWriter[Any](targetSchema)

  @transient private var encoder: BinaryEncoder = _

  @transient private lazy val out = new ByteArrayOutputStream

  /**
   * Instantiate serializer and writer for schema compatibility check
   */
  def test(): Unit = {
    serializer
    writer
  }

  override def nullSafeEval(input: Any): Any = {
    out.reset()
    appendSchemaId(id, out)
    encoder = EncoderFactory.get().directBinaryEncoder(out, encoder)
    val avroData = serializer.serialize(input)
    writer.write(avroData, encoder)
    encoder.flush()
    out.toByteArray
  }

  override def prettyName: String = "to_avro"

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val expr = ctx.addReferenceObj("this", this)
    defineCodeGen(ctx, ev, input => s"(byte[]) $expr.nullSafeEval($input)")
  }

  private def appendSchemaId(id: Int, os:ByteArrayOutputStream): Unit = {
    os.write(confluentHelper.CONFLUENT_MAGIC_BYTE)
    os.write(ByteBuffer.allocate(Integer.BYTES).putInt(id).array())
  }
}