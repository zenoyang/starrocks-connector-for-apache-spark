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

package com.starrocks.connector.spark.serialization;

import com.starrocks.connector.spark.exception.StarRocksException;
import com.starrocks.connector.spark.rest.models.FieldType;
import com.starrocks.connector.spark.sql.schema.StarRocksField;
import com.starrocks.connector.spark.sql.schema.StarRocksSchema;
import com.starrocks.format.Chunk;
import com.starrocks.format.Column;
import org.apache.spark.sql.types.Decimal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneId;
import java.util.TimeZone;
import java.util.function.BiFunction;

/**
 * row batch data container.
 */
public class BypassRowBatch extends BaseRowBatch {

    private static Logger logger = LoggerFactory.getLogger(BypassRowBatch.class);

    public BypassRowBatch(Chunk chunk, StarRocksSchema schema, ZoneId srTimezone) throws StarRocksException {
        super(schema, srTimezone);
        try {
            if (chunk.columnCount() != schema.getColumns().size()) {
                throw new StarRocksException(
                        String.format("column size mismatch, expect %d, but got %d in chunk.",
                                schema.getColumns().size(), chunk.columnCount())
                );
            }
            // init the rowBatch
            rowCountInOneBatch = chunk.numRow();
            for (int i = 0; i < rowCountInOneBatch; ++i) {
                rowBatch.add(new Row(schema.getColumns().size()));
            }
            convertChunkToRowBatch(schema, chunk, srTimezone);
            readRowCount += chunk.numRow();
        } catch (Exception e) {
            logger.error("Bypass read starrocks data failed, {}", e.getMessage(), e);
            if (e instanceof StarRocksException) {
                throw (StarRocksException) e;
            }
            throw new StarRocksException(e.getMessage());
        } finally {
            close();
        }
    }

    public void convertChunkToRowBatch(StarRocksSchema schema, Chunk chunk, ZoneId timeZone) {
        for (int idx = 0; idx < schema.getColumns().size(); idx++) {
            StarRocksField field = schema.getColumns().get(idx);
            Column column = chunk.getColumn(idx);
            switch (FieldType.of(field.getType())) {
                case NULL:
                    fillRowData(field, column, (col, i) -> null);
                    break;
                case BOOLEAN:
                    fillRowData(field, column, Column::getBoolean);
                    break;
                case TINYINT:
                    fillRowData(field, column, Column::getByte);
                    break;
                case SMALLINT:
                    fillRowData(field, column, Column::getShort);
                    break;
                case INT:
                    fillRowData(field, column, Column::getInt);
                    break;
                case BIGINT:
                    fillRowData(field, column, Column::getLong);
                    break;
                case LARGEINT:
                    fillRowData(field, column, (col, i) -> col.getLargeInt(i).toString());
                    break;
                case FLOAT:
                    fillRowData(field, column, Column::getFloat);
                    break;
                case DOUBLE:
                    fillRowData(field, column, Column::getDouble);
                    break;
                case DECIMAL:
                case DECIMALV2:
                case DECIMAL32:
                case DECIMAL64:
                case DECIMAL128:
                    fillRowData(field, column, (col, i) -> Decimal.apply(col.getDecimal(i)));
                    break;
                case CHAR:
                case VARCHAR:
                case JSON:
                    fillRowData(field, column, Column::getString);
                    break;
                case BINARY:
                case VARBINARY:
                    fillRowData(field, column, Column::getBinary);
                    break;
                case DATE:
                    fillRowData(field, column, Column::getDate);
                    break;
                case DATETIME:
                    fillRowData(field, column, (col, i) -> col.getTimestamp(i, TimeZone.getTimeZone(timeZone)));
                    break;
                default:
                    throw new StarRocksException("unsupported type for bypass read: " + field.getType());
            }
        }
    }

    private void fillRowData(StarRocksField srField, Column column, BiFunction<Column, Long, Object> function) {
        for (long rowIdx = 0L; rowIdx < rowCountInOneBatch; rowIdx++) {
            if (Boolean.FALSE.equals(srField.getNullable())) {
                addValueToRow(rowIdx, function.apply(column, rowIdx));
            } else {
                addValueToRow(rowIdx, column.isNullAt(rowIdx) ? null : function.apply(column, rowIdx));
            }
        }
    }

    @Override
    public void close() {

    }
}
