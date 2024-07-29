package com.starrocks.connector.spark.read

import com.starrocks.connector.spark.cfg.Settings
import com.starrocks.connector.spark.sql.schema.StarRocksSchema
import org.apache.spark.sql.connector.read.InputPartition


// For v2.0 Reader based on catalog
class StarRocksCatalogDecoder(partition: InputPartition, settings: Settings, schema: StarRocksSchema)
  extends AbstractStarRocksDecoder[StarRocksInternalRow](partition, settings, schema) {

  override def decode(values: Array[Any]) : StarRocksInternalRow = {
    new StarRocksInternalRow(values)
  }
}