package org.apache.spark.sql.confluent.avro

import io.confluent.kafka.schemaregistry.avro.AvroSchema
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.io.{BinaryEncoder, EncoderFactory}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Expression, UnaryExpression}
import org.apache.spark.sql.confluent.ConfluentClient
import org.apache.spark.sql.types.{BinaryType, DataType}

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer

// copied from org.apache.spark.sql.avro.*
case class CatalystDataToConfluentAvro(child: Expression, subject: String, confluentHelper: ConfluentClient[AvroSchema], updateAllowed: Boolean, mutualReadCheck: Boolean = false) extends UnaryExpression with Logging {
  if (!updateAllowed && mutualReadCheck) logWarning("mutualReadCheck is ignored if updateAllowed=false")

  override def dataType: DataType = BinaryType

  // prepare serializer and writer for avro schema of subject
  case class SerializerTools(schemaId: Int, serializer: MyAvroSerializer, writer: GenericDatumWriter[Any])

  @transient private lazy val tgt = {
    // Avro schema is not serializable in older versions. We must be careful to not store it in an attribute of the class.
    val newSchema = new AvroSchema(MySchemaConverters.toAvroType(child.dataType, child.nullable))
    val (schemaId, schema) = if (updateAllowed) confluentHelper.setOrUpdateSchema(subject, newSchema, mutualReadCheck)
    else confluentHelper.setOrGetSchema(subject, newSchema)
    val serializer = new MyAvroSerializer(child.dataType, schema.rawSchema, child.nullable)
    val writer = new GenericDatumWriter[Any](schema.rawSchema)
    SerializerTools(schemaId, serializer, writer)
  }

  @transient private var encoder: BinaryEncoder = _

  @transient private lazy val out = new ByteArrayOutputStream

  /**
   * Instantiate serializer and writer for schema compatibility check
   */
  def test(): Unit = {
    tgt // initialize lazy value
  }

  override def nullSafeEval(input: Any): Any = {
    out.reset()
    appendSchemaId(tgt.schemaId, out)
    encoder = EncoderFactory.get().directBinaryEncoder(out, encoder)
    val avroData = tgt.serializer.serialize(input)
    tgt.writer.write(avroData, encoder)
    encoder.flush()
    out.toByteArray
  }

  override def prettyName: String = "to_confluent_avro"

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val expr = ctx.addReferenceObj("this", this)
    defineCodeGen(ctx, ev, input => s"(byte[]) $expr.nullSafeEval($input)")
  }

  private def appendSchemaId(id: Int, os: ByteArrayOutputStream): Unit = {
    os.write(ConfluentAvroConnector.CONFLUENT_MAGIC_BYTE)
    os.write(ByteBuffer.allocate(Integer.BYTES).putInt(id).array())
  }

  override protected def withNewChildInternal(newChild: Expression): CatalystDataToConfluentAvro = copy(child = newChild)
}
