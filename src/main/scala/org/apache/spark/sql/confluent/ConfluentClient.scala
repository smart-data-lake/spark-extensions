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

package org.apache.spark.sql.confluent

import io.confluent.kafka.schemaregistry.{ParsedSchema, SchemaProvider}
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider
import io.confluent.kafka.schemaregistry.client.{CachedSchemaRegistryClient, SchemaRegistryClient}
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Column
import org.apache.spark.sql.confluent.SubjectType.SubjectType

import scala.jdk.CollectionConverters._
import scala.collection.mutable

/**
 * Wrapper for schema registry client.
 * It supports advanced logic for schema compatibility check and update.
 *
 * @tparam S schema type, e.g. AvroSchema or JsonSchema
 */
class ConfluentClient[S <: ParsedSchema](schemaRegistryUrl: String) extends Logging with Serializable {

  @transient lazy val sr: SchemaRegistryClient = new CachedSchemaRegistryClient(Seq(schemaRegistryUrl).asJava, 1000, Seq[SchemaProvider](new AvroSchemaProvider, new JsonSchemaProvider).asJava, null)
  @transient private lazy val subjects = mutable.Set(sr.getAllSubjects.asScala.toSeq: _*)
  logInfo(s"Initialize confluent schema registry with url $schemaRegistryUrl")

  /**
   * get the subject name for a topic (confluent standard is postfixing '-key' or '-value')
   */
  def getSubject(topic: String, subjectType: SubjectType): String = subjectType match {
    case SubjectType.key => s"$topic-key"
    case SubjectType.value => s"$topic-value"
  }

  /**
   * test connection by calling a simple function
   */
  def test(): Unit = sr.getMode

  /**
   * If schema already exists, update if compatible, otherwise create schema
   * Throws exception if new schema is not compatible.
   *
   * @return confluent schemaId and registered schema
   */
  def setOrUpdateSchema(subject: String, newSchema: S, mutualReadCheck: Boolean = false): (Int, S) = {
    if (!schemaExists(subject)) return registerSchema(subject, newSchema)
    val latestSchema = getLatestSchemaFromConfluent(subject)
    if (!latestSchema._2.equals(newSchema)) {
      val checkSchemaFunc = if (mutualReadCheck) checkSchemaMutualReadable _ else checkSchemaCanRead _
      val compatibilityViolations = checkSchemaFunc(latestSchema._2, newSchema)
      val modeForLogging = if (mutualReadCheck) "forward & backward" else "backward"
      if (compatibilityViolations.isEmpty) {
        logInfo(s"New schema for subject $subject is $modeForLogging compatible with latest schema: new=$newSchema")
        registerSchema(subject, newSchema)
      } else {
        val msg = s"New schema for subject $subject is not $modeForLogging compatible with latest schema"
        logError(s"$msg: latest=${latestSchema._2} new=$newSchema violations=${compatibilityViolations.mkString(";")}")
        throw new IncompatibleSchemaException(msg)
      }
    } else {
      logDebug(s"New schema for $subject is equal to latest schema")
      latestSchema
    }
  }

  /**
   * Get existing schema, otherwise create schema
   *
   * @return confluent schemaId and registered schema
   */
  def setOrGetSchema(subject: String, newSchema: S): (Int, S) = {

    if (!schemaExists(subject)) return registerSchema(subject, newSchema)
    getLatestSchemaFromConfluent(subject)
  }

  def getLatestSchemaFromConfluent(subject: String): (Int, S) = {
    val m = sr.getLatestSchemaMetadata(subject)
    getSchemaFromConfluent(m.getId)
  }

  def getSchemaFromConfluent(id: Int): (Int, S) = {
    val avroSchema = sr.getSchemaById(id).asInstanceOf[S]
    (id, avroSchema)
  }

  private def registerSchema(subject: String, schema: S): (Int, S) = {
    logInfo(s"Register new schema for $subject: schema=$schema")
    val schemaId = sr.register(subject, schema.asInstanceOf[ParsedSchema])
    subjects.add(subject)
    (schemaId, schema)
  }

  private def schemaExists(subject: String): Boolean = {
    subjects.contains(subject)
  }

  /**
   * Check if the data produced by existing schema can be read by the new schema.
   * It allows readers to read old & new messages with the new schema.
   * This is sufficient for schema evolution if all readers can be easily migrated to read using the new schema.
   * @return list of compatibility violations
   **/
  protected def checkSchemaCanRead(dataSchema: S, readSchema: S): Seq[String] = {
    readSchema.isBackwardCompatible(dataSchema).asScala.toSeq
  }

  /**
   * Check if the data produced by an existing schema can be read by the new schema and vice versa
   * It allows readers to keep using the old schema and reading messages with the new schema.
   * @return list of compatibility violations
   **/
  protected def checkSchemaMutualReadable(schema1: S, schema2: S): Seq[String] = {
    (schema1.isBackwardCompatible(schema2).asScala ++ schema2.isBackwardCompatible(schema1).asScala).toSeq
  }
}


trait ConfluentConnector extends Serializable {

  /**
   * Convert a column from confluent format to Spark.
   * @param data the binary column.
   * @param topic the topic name.
   * @param subjectType the subject type (key or value).
   */
  def from_confluent(data: Column, topic: String, subjectType: SubjectType): Column

  /**
   * Convert a column from Spark to confluent format.
   * @param data the data column.
   * @param topic the topic name.
   * @param subjectType the subject type (key or value).
   * @param updateAllowed if subject schema should be updated if compatible
   * @param mutualReadCheck if a mutual read check or a simpler can read check should be executed
   * @param eagerCheck if true tiggers instantiation of converter object instances (only if connector uses such mechanisms)
   */
  def to_confluent(data: Column, topic: String, subjectType: SubjectType, updateAllowed: Boolean = false, mutualReadCheck: Boolean = false, eagerCheck: Boolean = false): Column

}


object SubjectType extends Enumeration {
  type SubjectType = Value
  val key, value = Value
}

class IncompatibleSchemaException(msg: String, ex: Throwable = null) extends Exception(msg, ex)
