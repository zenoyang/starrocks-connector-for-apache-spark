// Modifications Copyright 2021 StarRocks Limited.
//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.connector.spark.rdd

import com.starrocks.connector.spark.cfg.ConfigurationOptions.{STARROCKS_FORMAT_QUERY_PLAN, removePrefix}
import com.starrocks.connector.spark.cfg.Settings
import com.starrocks.connector.spark.rest.BypassPartition
import com.starrocks.connector.spark.serialization.BypassRowBatch
import com.starrocks.connector.spark.sql.schema.{StarRocksField, StarRocksSchema}
import com.starrocks.connector.spark.util.{ConfigUtils, FieldUtils}
import com.starrocks.format.StarRocksReader
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory

import java.util
import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.JavaConverters._


/**
 * read data from Starrocks BE to array.
 *
 * @param partition Starrocks RDD partition
 * @param settings  request configuration
 */
class BypassValueReader(partition: BypassPartition, settings: Settings, schema: StarRocksSchema)
  extends BaseValueReader(partition, settings, schema) {

  private val log = LoggerFactory.getLogger(classOf[BypassValueReader])

  private val eos: AtomicBoolean = new AtomicBoolean(false)
  private var outputSchema: StarRocksSchema = _
  private var reader: StarRocksReader = _

  override def init(): Unit = {
    val fields = Option(partition.getSelectClause) match {
      case Some(f) => f.split(""",\s*""").map(fn => FieldUtils.eraseQuote(fn)).toSeq
      case None => Seq.empty[String]
    }

    val nameToFieldMapping = schema.getColumns.asScala.map(t => t.getName -> t).toMap
    val outputFields = fields.map(fn => nameToFieldMapping.get(fn) match {
      case Some(field) => field
      case None => throw new IllegalStateException(s"filed '${fn}' not found in schema")
    }).toList.asJava

    outputSchema = new StarRocksSchema(outputFields)
    val requiredFieldNames = partition.getRequiredFieldNames
    val outputColumnNames = outputSchema.getColumns.asScala.map(_.getName).toList.asJava

    settings.removeProperty(STARROCKS_FORMAT_QUERY_PLAN)
    if (ConfigUtils.isFilterPushDownEnabled(settings) && StringUtils.isNotBlank(partition.getFilterClause)) {
      settings.setProperty(STARROCKS_FORMAT_QUERY_PLAN, partition.getQueryPlan)
    }

    val startMillis = System.currentTimeMillis()
    val etlTable = schema.getEtlTable
    reader = new StarRocksReader(
      partition.getTabletId,
      partition.getVersion,
      etlTable.toPbTabletSchema(requiredFieldNames.asScala.toList.asJava),
      etlTable.toPbTabletSchema(outputColumnNames),
      partition.getStoragePath,
      removePrefix(settings.getPropertyMap))
    reader.open()

    if (ConfigUtils.isVerbose(settings)) {
      val elapseMillis = System.currentTimeMillis() - startMillis
      Option(settings.getProperty(STARROCKS_FORMAT_QUERY_PLAN)) match {
        case Some(plan) => log.info(
          s"Open native reader for tablet ${partition.getTabletId} use ${elapseMillis}ms with query plan:\n$plan")
        case None => log.info(
          s"Open native reader for tablet ${partition.getTabletId} use ${elapseMillis}ms without query plan.")
      }

    }
  }

  override def hasNext: Boolean = {
    rowHasNext = rowBatch == null || rowBatch.hasNext
    // Arrow data was acquired synchronously during the iterative process
    if (!eos.get && (rowBatch == null || !rowBatch.hasNext)) {
      if (rowBatch != null) {
        rowBatch.close()
      }

      val chunk = reader.getNext
      try {
        eos.set(chunk.numRow() == 0)
        rowHasNext = chunk.numRow() != 0
        if (!eos.get) {
          rowBatch = new BypassRowBatch(chunk, outputSchema, srTimeZone)
        }
      } finally {
        // FIXME maybe chunk should be reuse
        chunk.release()
      }
    }

    rowBatchHasNext = !eos.get
    rowBatchHasNext
  }

  override def close(): Unit = {
    reader.close()
    reader.release()
  }
}
