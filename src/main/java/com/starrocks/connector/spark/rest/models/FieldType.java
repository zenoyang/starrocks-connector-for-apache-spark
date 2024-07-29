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

package com.starrocks.connector.spark.rest.models;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public enum FieldType {

    NULL("NULL_TYPE"),
    BOOLEAN("BOOLEAN"),
    TINYINT("TINYINT"),
    SMALLINT("SMALLINT"),
    INT("INT"),
    BIGINT("BIGINT"),
    LARGEINT("LARGEINT"),
    FLOAT("FLOAT"),
    DOUBLE("DOUBLE"),
    DECIMAL("DECIMAL"),
    DECIMALV2("DECIMALV2"),
    DECIMAL32("DECIMAL32"),
    DECIMAL64("DECIMAL64"),
    DECIMAL128("DECIMAL128"),
    CHAR("CHAR"),
    VARCHAR("VARCHAR"),
    BINARY("BINARY"),
    VARBINARY("VARBINARY"),
    DATE("DATE"),
    DATETIME("DATETIME"),
    TIME("TIME"),
    JSON("JSON"),
    ARRAY("ARRAY"),
    HLL("HLL"),
    BITMAP("BITMAP");

    private static final Map<String, FieldType> REVERSE_MAPPING = new HashMap<>(values().length);

    static {
        for (FieldType fieldType : values()) {
            if (REVERSE_MAPPING.containsKey(fieldType.getTypeName().toUpperCase())) {
                throw new IllegalStateException("duplicated type: " + fieldType.getTypeName());
            }

            REVERSE_MAPPING.put(fieldType.getTypeName().toUpperCase(), fieldType);
        }
    }

    public static FieldType of(String typeName) {
        return elegantOf(typeName)
                .orElseThrow(() -> new IllegalArgumentException("unknown field type: " + typeName));
    }

    public static Optional<FieldType> elegantOf(String typeName) {
        return Optional.ofNullable(typeName)
                .map(String::toUpperCase)
                .map(REVERSE_MAPPING::get);
    }

    public static boolean isSupported(String typeName) {
        return elegantOf(typeName).isPresent();
    }

    public static boolean isUnsupported(String typeName) {
        return !isSupported(typeName);
    }

    private final String typeName;

    FieldType(String typeName) {
        this.typeName = typeName;
    }

    public String getTypeName() {
        return typeName;
    }
}
