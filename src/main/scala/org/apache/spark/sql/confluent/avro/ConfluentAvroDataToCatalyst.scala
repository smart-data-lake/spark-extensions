package org.apache.spark.sql.confluent.avro

import io.confluent.kafka.schemaregistry.avro.AvroSchema
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.io.{BinaryDecoder, DecoderFactory}
import org.apache.spark.sql.avro.AvroDeserializer
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode}
import org.apache.spark.sql.confluent.ConfluentClient
import org.apache.spark.sql.internal.SQLConf.LegacyBehaviorPolicy
import org.apache.spark.sql.types.{AbstractDataType, BinaryType, DataType}

import java.nio.ByteBuffer
import scala.collection.mutable

// copied from org.apache.spark.sql.avro.*
case class ConfluentAvroDataToCatalyst(child: Expression, subject: String, confluentHelper: ConfluentClient[AvroSchema])
  extends UnaryExpression with ExpectsInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(BinaryType)

  override lazy val dataType: DataType = {
    // Avro schema is not serializable in older versions. We must be careful to not store it in an attribute of the class.
    val (schemaId, schema) = confluentHelper.getLatestSchemaFromConfluent(subject)
    MySchemaConverters.toSqlType(schema.rawSchema).dataType
  }

  override def nullable: Boolean = true

  // To read an avro message we need to use the schema referenced by the message. Therefore we might need different readers for different messages.
  private val avroReaders = mutable.Map[Int, GenericDatumReader[Any]]()
  // To deserialize a generic avro message to a Spark row we need to use the Avro schema referenced by the message. Therefore we might need different deserializers for different messages.
  private val avro2SparkDeserializers = mutable.Map[Int, AvroDeserializer]()
  // buffer objects for reuse
  private var avroBinaryDecoder: BinaryDecoder = _
  private var avroGenericMsg: Any = _

  override def nullSafeEval(input: Any): Any = {
    val binary = input.asInstanceOf[Array[Byte]]
    val (schemaId, avroMsg) = parseConfluentMsg(binary)
    val (_, msgSchema) = confluentHelper.getSchemaFromConfluent(schemaId)
    avroBinaryDecoder = DecoderFactory.get().binaryDecoder(avroMsg, 0, avroMsg.length, avroBinaryDecoder)
    val avroReader = avroReaders.getOrElseUpdate(schemaId, new GenericDatumReader[Any](msgSchema.rawSchema))
    avroGenericMsg = avroReader.read(avroGenericMsg, avroBinaryDecoder)
    val avro2SparkDeserializer = avro2SparkDeserializers.getOrElseUpdate(schemaId, new AvroDeserializer(msgSchema.rawSchema, dataType))
    avro2SparkDeserializer.deserialize(avroGenericMsg).orNull
  }

  override def prettyName: String = "from_confluent_avro"

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val expr = ctx.addReferenceObj("this", this)
    defineCodeGen(ctx, ev, input =>
      s"(${CodeGenerator.boxedType(dataType)})$expr.nullSafeEval($input)")
  }

  def parseConfluentMsg(msg: Array[Byte]): (Int, Array[Byte]) = {
    val msgBuffer = ByteBuffer.wrap(msg)
    val magicByte = msgBuffer.get
    require(magicByte == ConfluentAvroConnector.CONFLUENT_MAGIC_BYTE, "Magic byte not present at start of confluent message!")
    val schemaId = msgBuffer.getInt
    val avroMsg = msg.slice(msgBuffer.position, msgBuffer.limit)
    //return
    (schemaId, avroMsg)
  }
}
