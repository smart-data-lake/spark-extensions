package org.apache.spark.sql.confluent.avro

import io.confluent.kafka.schemaregistry.avro.AvroSchema
import org.apache.avro.{Schema, SchemaValidatorBuilder}
import org.apache.spark.sql.Column
import org.apache.spark.sql.confluent.{ConfluentClient, ConfluentConnector}
import org.apache.spark.sql.confluent.SubjectType.SubjectType

import scala.jdk.CollectionConverters._
import scala.collection.mutable

/**
 * Provides Spark SQL functions from/to_confluent_avro for decoding/encoding confluent avro messages.
 */
class ConfluentAvroConnector(confluentClient: ConfluentClient[AvroSchema]) extends ConfluentConnector {

  /**
   * Converts a binary column of confluent avro format into its corresponding catalyst value according to the latest
   * schema stored in schema registry.
   * Note: copied from org.apache.spark.sql.avro.*
   *
   * @param data the binary column.
   * @param topic the topic name.
   * @param subjectType the subject type (key or value).
   */
  override def from_confluent(data: Column, topic: String, subjectType: SubjectType): Column = {
    val subject = confluentClient.getSubject(topic, subjectType)
    new Column(ConfluentAvroDataToCatalyst(data.expr, subject, confluentClient))
  }

  /**
   * Converts a column into binary of confluent avro format according to the latest schema stored in schema registry.
   * Note: copied from org.apache.spark.sql.avro.*
   *
   * @param data the data column.
   * @param topic the topic name.
   * @param subjectType the subject type (key or value).
   * @param updateAllowed if subject schema should be updated if compatible
   * @param eagerCheck if true tiggers instantiation of converter object instances
   */
  override def to_confluent(data: Column, topic: String, subjectType: SubjectType, updateAllowed: Boolean = false, mutualReadCheck: Boolean = false, eagerCheck: Boolean = false): Column = {
    val subject = confluentClient.getSubject(topic, subjectType)
    val converterExpr = CatalystDataToConfluentAvro(data.expr, subject, confluentClient, updateAllowed, mutualReadCheck )
    if (eagerCheck) converterExpr.test()
    new Column(converterExpr)
  }

  protected def debugSchemaDiff(schema1: AvroSchema, schema2: AvroSchema): Seq[String] = {
    debugSchemaDiff(schema1.rawSchema, schema2.rawSchema)
  }

  private def debugSchemaDiff(schema1: Schema, schema2: Schema, fieldName: Option[String] = None): Seq[String] = {
    //if (schema1.getName!=schema2.getName) Seq(s"schema names don't match (${schema1.getName}, ${schema2.getName})")
    if (!debugTypeIsCompatible(schema1.getType, schema2.getType)) Seq(s"""${fieldName.map("("+_+")").getOrElse("")} schema types don't match (${schema1.getType}, ${schema2.getType})""")
    else {
      if (schema1.getType==Schema.Type.RECORD) {
        val diffs = mutable.Buffer[String]()
        val fields1 = schema1.getFields.asScala.map( f => (f.name,f.schema)).toMap
        val fields2 = schema2.getFields.asScala.map( f => (f.name,f.schema)).toMap
        val f1m2 = fields1.keys.toSeq.diff(fields2.keys.toSeq)
        if (f1m2.nonEmpty) diffs += s"""fields ${f1m2.mkString(", ")} missing in schema2"""
        val f2m1 = fields2.keys.toSeq.diff(fields1.keys.toSeq)
        if (f2m1.nonEmpty) diffs += s"""fields ${f2m1.mkString(", ")} missing in schema1"""
        diffs ++= fields1.keys.toSeq.intersect(fields2.keys.toSeq).flatMap( f => debugSchemaDiff(debugSchemaReduceUnion(fields1(f)),debugSchemaReduceUnion(fields2(f)), Some(f)))
        diffs.toSeq
      } else Seq()
    }
  }

  private def debugSchemaReduceUnion(schema: Schema): Schema = {
    if (schema.getType==Schema.Type.UNION) {
      val filteredSubSchemas = schema.getTypes.asScala.filter( _.getType!=Schema.Type.NULL )
      if (filteredSubSchemas.size==1) filteredSubSchemas.head
      else schema
    } else schema
  }

  private def debugTypeIsCompatible(type1: Schema.Type, type2: Schema.Type) = (type1, type2) match {
    case (Schema.Type.STRING, Schema.Type.ENUM) => true
    case (Schema.Type.ENUM, Schema.Type.STRING) => true
    case _ => type1 == type2
  }
}

object ConfluentAvroConnector {
  val CONFLUENT_MAGIC_BYTE = 0x0 // magic byte needed as first byte of confluent binary kafka message

  def apply(schemaRegistryUrl: String): ConfluentAvroConnector = {
    new ConfluentAvroConnector(new AvroConfluentClient(schemaRegistryUrl))
  }

  def parseAvroSchema(schema: String): Schema = {
    new Schema.Parser().parse(schema)
  }
}

class AvroConfluentClient(schemaRegistryUrl: String) extends ConfluentClient[AvroSchema](schemaRegistryUrl) {

  override protected def checkSchemaCanRead(dataSchema: AvroSchema, readSchema: AvroSchema): Seq[String] = {
    val validatorRead = new SchemaValidatorBuilder().canReadStrategy.validateLatest
    try {
      validatorRead.validate(readSchema.rawSchema, Seq(dataSchema.rawSchema).asJava)
      Seq()
    } catch {
      case e: Exception => Seq(s"${e.getClass.getSimpleName}: ${e.getMessage}")
    }
  }

  override protected def checkSchemaMutualReadable(schema1: AvroSchema, schema2: AvroSchema): Seq[String] = {
    val validatorRead = new SchemaValidatorBuilder().mutualReadStrategy().validateLatest()
    try {
      validatorRead.validate(schema2.rawSchema, Seq(schema1.rawSchema).asJava)
      Seq()
    } catch {
      case e: Exception => Seq(s"${e.getClass.getSimpleName}: ${e.getMessage}")
    }
  }
}