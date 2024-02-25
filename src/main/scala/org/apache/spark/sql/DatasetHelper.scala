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

package org.apache.spark.sql

import org.apache.spark.SparkEnv
import org.apache.spark.rdd.BlockRDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.{StorageLevel, TempLocalBlockId}

import java.util.UUID

object DatasetHelper {

  /**
   * Show Dataset but return string instead of writing to stdout
   **/
  def showString(ds: Dataset[_], numRows: Int = 20, truncate: Int = 20, vertical: Boolean = false): String = {
    ds.showString(numRows, truncate, vertical)
  }

  /**
   * Create a DataFrame from data available as Iterator of InternalRows on the driver node.
   * Memory usage is optimized by using an Iterator and splitting partitions according to maxRowsPerPartition.
   *
   * The function creates Blocks using BlockManager and StorageLevel.MEMORY_AND_DISK_SER by default.
   * If blocks no longer fit into driver memory, they should be offloaded to disk.
   *
   * @return DataFrame with given schema
   */
  def parallelizeInternalRows(rows: Iterator[InternalRow], schema: StructType, maxRowsPerPartition: Int, storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER)(implicit session: SparkSession): DataFrame = {
    // split data into blocks to avoid getting out of memory, save blocks to Spark BlockManager
    val blockIds = rows
      .grouped(maxRowsPerPartition)
      .map {
        rowGroup =>
          val blockId = TempLocalBlockId(UUID.randomUUID())
          SparkEnv.get.blockManager.putIterator(blockId, rowGroup.toIterator, StorageLevel.MEMORY_AND_DISK_SER)
          blockId
      }
    // create RDD and DataFrame from blocks
    val rdd = new BlockRDD[InternalRow](session.sparkContext, blockIds.toArray)
    session.internalCreateDataFrame(rdd, schema)
  }

}
