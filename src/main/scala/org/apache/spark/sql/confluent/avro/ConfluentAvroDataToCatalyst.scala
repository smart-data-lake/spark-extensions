package org.apache.spark.sql.confluent.avro

import io.confluent.kafka.schemaregistry.avro.AvroSchema
import org.apache.avro.Schema
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.io.{BinaryDecoder, DecoderFactory}
import org.apache.spark.SparkException
import org.apache.spark.sql.avro.{AvroDeserializer, AvroOptions, SchemaConverters}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, SpecificInternalRow, UnaryExpression}
import org.apache.spark.sql.catalyst.util.{FailFastMode, ParseMode, PermissiveMode}
import org.apache.spark.sql.confluent.ConfluentClient
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.types.{AbstractDataType, BinaryType, DataType, StructType}

import java.nio.ByteBuffer
import scala.collection.mutable
import scala.util.control.NonFatal

// copied from org.apache.spark.sql.avro.AvroDataToCatalyst
case class ConfluentAvroDataToCatalyst(child: Expression, subject: String, confluentHelper: ConfluentClient[AvroSchema], avroOptions: AvroOptions)
  extends UnaryExpression with ExpectsInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(BinaryType)

  // Avro schema is not serializable in older versions. We must be careful to not store it in an attribute of the class.
  @transient private lazy val subjectSchema = {
    val originalSchema = confluentHelper.getLatestSchemaFromConfluent(subject)._2
    val fixedSchema = new AvroSchema(AvroHelper.fixNullableDefault(originalSchema.rawSchema()))
    println(originalSchema)
    println(fixedSchema)
    fixedSchema
  }

  override lazy val dataType: DataType = {
    val dt = SchemaConverters.toSqlType(
      subjectSchema.rawSchema,
      avroOptions.useStableIdForUnionType,
      avroOptions.stableIdPrefixForUnionType,
      avroOptions.recursiveFieldMaxDepth).dataType
    parseMode match {
      // With PermissiveMode, the output Catalyst row might contain columns of null values for
      // corrupt records, even if some of the columns are not nullable in the user-provided schema.
      // Therefore we force the schema to be all nullable here.
      case PermissiveMode => dt.asNullable
      case _ => dt
    }
  }

  override def nullable: Boolean = true

  // To read an avro message we need to use the schema referenced by the message. Therefore we might need different readers for different messages.
  private val avroReaders = mutable.Map[Int, GenericDatumReader[Any]]()
  // To deserialize a generic avro message to a Spark row we need to use the Avro schema referenced by the message. Therefore we might need different deserializers for different messages.
  private val avroDeserializers = mutable.Map[Int, AvroDeserializer]()

  // buffer objects for reuse
  private var decoder: BinaryDecoder = _
  private var result: Any = _

  @transient private lazy val parseMode: ParseMode = {
    val mode = avroOptions.parseMode
    if (mode != PermissiveMode && mode != FailFastMode) {
      throw QueryCompilationErrors.parseModeUnsupportedError(
        prettyName, mode
      )
    }
    mode
  }

  @transient private lazy val nullResultRow: Any = dataType match {
    case st: StructType =>
      val resultRow = new SpecificInternalRow(st.map(_.dataType))
      for (i <- 0 until st.length) {
        resultRow.setNullAt(i)
      }
      resultRow

    case _ =>
      null
  }

  override def nullSafeEval(input: Any): Any = {
    val binary = input.asInstanceOf[Array[Byte]]
    try {
      val (schemaId, avroMsg) = parseConfluentMsg(binary)
      val (_, msgSchema) = confluentHelper.getSchemaFromConfluent(schemaId)
      decoder = DecoderFactory.get().binaryDecoder(avroMsg, 0, avroMsg.length, decoder)
      val reader = avroReaders.getOrElseUpdate(schemaId, new GenericDatumReader[Any](msgSchema.rawSchema,subjectSchema.rawSchema()))
      result = reader.read(result, decoder)
      val deserializer = avroDeserializers.getOrElseUpdate(
        schemaId,
        new AvroDeserializer(subjectSchema.rawSchema, dataType, datetimeRebaseMode = avroOptions.datetimeRebaseModeInRead, useStableIdForUnionType = avroOptions.useStableIdForUnionType, stableIdPrefixForUnionType = avroOptions.stableIdPrefixForUnionType, recursiveFieldMaxDepth = avroOptions.recursiveFieldMaxDepth)
      )
      val deserialized = deserializer.deserialize(result)
      assert(deserialized.isDefined,
        "Avro deserializer cannot return an empty result because filters are not pushed down")
      deserialized.get
    } catch {
      // There could be multiple possible exceptions here, e.g. java.io.IOException,
      // AvroRuntimeException, ArrayIndexOutOfBoundsException, etc.
      // To make it simple, catch all the exceptions here.
      case NonFatal(e) => parseMode match {
        case PermissiveMode => nullResultRow
        case FailFastMode =>
          throw new SparkException("Malformed records are detected in record parsing. " +
            s"Current parse Mode: ${FailFastMode.name}. To process malformed records as null " +
            "result, try setting the option 'mode' as 'PERMISSIVE'.", e)
        case _ =>
          throw QueryCompilationErrors.parseModeUnsupportedError(
            prettyName, parseMode
          )
      }
    }
  }

  override def prettyName: String = "from_confluent_avro"

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val expr = ctx.addReferenceObj("this", this)
    nullSafeCodeGen(ctx, ev, eval => {
      val result = ctx.freshName("result")
      val dt = CodeGenerator.boxedType(dataType)
      s"""
        $dt $result = ($dt) $expr.nullSafeEval($eval);
        if ($result == null) {
          ${ev.isNull} = true;
        } else {
          ${ev.value} = $result;
        }
      """
    })
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

  override protected def withNewChildInternal(newChild: Expression): ConfluentAvroDataToCatalyst = copy(child = newChild)
}

object AvroHelper {
  def fixNullableDefault(schema: Schema): Schema = {
    import scala.jdk.CollectionConverters._
    if (schema.getType ne Schema.Type.NULL) {
      val fields = schema.getFields.asScala.map { field =>
        if (field.schema.getType eq Schema.Type.UNION) {
          val nullTpeExists = field.schema.getTypes.asScala.exists(_.getType eq Schema.Type.NULL)
          val fixedDefaultValue = if (nullTpeExists && field.defaultVal == null) {
            Schema.Field.NULL_DEFAULT_VALUE
          } else field.defaultVal()
          val fields = field.schema.getTypes.asScala.map{ fieldSchema =>
            if (fieldSchema.getType eq Schema.Type.RECORD) fixNullableDefault(fieldSchema)
            else if (fieldSchema.getType eq Schema.Type.ARRAY) {
              val elementType = fixNullableDefault(fieldSchema.getElementType)
              Schema.createArray(elementType)
            } else fieldSchema
          }.sortBy(_.getType eq Schema.Type.NULL).reverse // sort null type frist
          .asJava
          new Schema.Field(field.name, Schema.createUnion(fields), field.doc, fixedDefaultValue)
        } else new Schema.Field(field.name, field.schema, field.doc, field.defaultVal)
      }
      Schema.createRecord(schema.getName, schema.getDoc, schema.getNamespace, false, fields.asJava)
    } else schema
  }
}