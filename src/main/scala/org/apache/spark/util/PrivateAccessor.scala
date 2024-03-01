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

package org.apache.spark.util

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil

/**
 * Make some private Spark methods available in SDLB.
 */
object PrivateAccessor {

  /**
   * Create a Hadoop Configuration with Spark properties applied on top.
   */
  def getHadoopConfiguration(properties: Map[String,String]): Configuration = {
    val sparkConf = new SparkConf().setAll(properties)
    SparkHadoopUtil.get.newConfiguration(sparkConf)
  }
}
