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

package org.apache.spark.sql.custom

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.{Encoders, Row}

import scala.reflect.runtime.universe._

/**
 * Spark row to case class Decoder.
 * This can be used for example in UDF's with struct parameters, which are mapped as Row in Spark 2.x (Spark 3.x supports case classes as parameters directly)
 * If you have to decode multiple rows (like in a UDF) it's important to instantiate the RowDecoder only once (outside the UDF) and reuse it for all rows.
 *
 * @tparam T the case class to be produced
 */
class RowDecoder[T <: Product : TypeTag] extends Serializable {

  private val encoder = Encoders.product[T].asInstanceOf[ExpressionEncoder[T]]
  private val internalRowConverter = CatalystTypeConverters.createToCatalystConverter(encoder.schema)
  private val resolvedEncoder = encoder.resolveAndBind(encoder.schema.toAttributes)
  private val rowDeserializer = resolvedEncoder.createDeserializer

  /**
   * Decode Spark row to case class
   */
  def convert(row: Row): T = {
    rowDeserializer(internalRowConverter(row).asInstanceOf[InternalRow])
  }
}