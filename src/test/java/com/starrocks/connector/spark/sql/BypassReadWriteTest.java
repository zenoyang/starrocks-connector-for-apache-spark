// Copyright 2021-present StarRocks, Inc. All rights reserved.
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

package com.starrocks.connector.spark.sql;

import com.google.common.collect.Lists;
import com.starrocks.connector.spark.sql.conf.ReadStarRocksConfig.ReadMode;
import com.starrocks.connector.spark.sql.conf.WriteStarRocksConfig.WriteMode;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.starrocks.connector.spark.read.StarRocksScan.filterClauseForTest;
import static com.starrocks.connector.spark.read.StarRocksScan.resetFilterClauseForTest;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class BypassReadWriteTest extends BypassModeTestBase {

    @BeforeEach
    public void beforeEach() throws Exception {
        ITTestBase.beforeClass();

        clean();

        executeSrSQL(String.format("CREATE DATABASE IF NOT EXISTS %s", DB_NAME));

        for (String table : TABLES) {
            executeSrSQL(loadSql(table));
        }
    }

    @AfterEach
    public void afterEach() throws Exception {
        clean();
        ITTestBase.afterClass();
    }

    @ParameterizedTest
    @MethodSource("initMutableParams")
    public void testDuplicateKeyTable(boolean useCatalog, ReadMode readMode, WriteMode writeMode) throws Throwable {
        long timestamp = System.currentTimeMillis();
        withSparkSession(spark -> {
            List<List<Object>> expectedData = new ArrayList<>();
            expectedData.add(Arrays.asList(timestamp, 1001L, "LOGIN", "SUCCESS", ""));
            expectedData.add(Arrays.asList(
                    timestamp, 1002L, "REGISTER", "FAILURE", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"));
            expectedData.add(Arrays.asList(timestamp, 1003L, "LOGOUT", "SUCCESS", ""));

            String insertSql = String.format(
                    "INSERT INTO %s.%s VALUES " +
                            "(" + timestamp + ", 1001, 'LOGIN', 'SUCCESS', ''), " +
                            "(" + timestamp +
                            ", 1002, 'REGISTER', 'FAILURE', 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)'), " +
                            "(" + timestamp + ", 1003, 'LOGOUT', 'SUCCESS', '')",
                    DB_NAME, TB_DUPLICATE_KEY);
            spark.sql(insertSql);

            String selectSql = String.format("SELECT * FROM %s.%s", DB_NAME, TB_DUPLICATE_KEY);
            Dataset<Row> df = spark.sql(selectSql);
            df.show();
            verifyRows(expectedData, df.collectAsList());
        }, useCatalog, readMode, writeMode);
    }

    @ParameterizedTest
    @MethodSource("initMutableParams")
    public void testAggregateKeyTable(boolean useCatalog, ReadMode readMode, WriteMode writeMode) throws Throwable {
        withSparkSession(spark -> {
            List<List<Object>> expectedData = new ArrayList<>();
            expectedData.add(Arrays.asList(1001, "google.com", 2));
            expectedData.add(Arrays.asList(1002, "bytedance.com", 7));
            expectedData.add(Arrays.asList(1003, "apache.org", 4));

            String insertSql = String.format("INSERT INTO %s.%s VALUES " +
                            "(1001, 'google.com', 2), " +
                            "(1002, 'bytedance.com', 3), " +
                            "(1003, 'apache.org', 4), " +
                            "(1002, 'bytedance.com', 4)",
                    DB_NAME, TB_AGGREGATE_KEY);
            spark.sql(insertSql);

            Dataset<Row> df = spark.sql(String.format("SELECT * FROM %s.%s", DB_NAME, TB_AGGREGATE_KEY));
            df.show();
            verifyRows(expectedData, df.collectAsList());
        }, useCatalog, readMode, writeMode);
    }

    @ParameterizedTest
    @MethodSource("initMutableParams")
    public void testUniqueKeyTable(boolean useCatalog, ReadMode readMode, WriteMode writeMode) throws Throwable {
        withSparkSession(spark -> {
            String selectSql = String.format("SELECT * FROM %s.%s", DB_NAME, TB_UNIQUE_KEY);

            {
                List<List<Object>> expectedData = new ArrayList<>();
                expectedData.add(Arrays.asList(1001, "google.com", 2));
                expectedData.add(Arrays.asList(1002, "bytedance.com", 4));
                expectedData.add(Arrays.asList(1003, "apache.org", 4));

                String insertSql = String.format("INSERT INTO %s.%s VALUES " +
                                "(1001, 'google.com', 2), " +
                                "(1002, 'bytedance.com', 3), " +
                                "(1003, 'apache.org', 4), " +
                                "(1002, 'bytedance.com', 4)",
                        DB_NAME, TB_UNIQUE_KEY);
                spark.sql(insertSql);

                Dataset<Row> df = spark.sql(selectSql);
                df.show();
                verifyRows(expectedData, df.collectAsList());
            }

            // unique key overwrite key
            {
                List<List<Object>> expectedData = new ArrayList<>();
                expectedData.add(Arrays.asList(1001, "google.com", 2));
                expectedData.add(Arrays.asList(1002, "bytedance.com", 5));
                expectedData.add(Arrays.asList(1003, "apache.org", 4));

                String insertSql = String.format("INSERT INTO %s.%s VALUES (1002, 'bytedance.com', 5)", DB_NAME, TB_UNIQUE_KEY);
                spark.sql(insertSql);

                Dataset<Row> df = spark.sql(selectSql);
                df.show();
                verifyRows(expectedData, df.collectAsList());
            }
        }, useCatalog, readMode, writeMode);
    }

    @ParameterizedTest
    @MethodSource("initMutableParams")
    public void testPrimaryKeyTable(boolean useCatalog, ReadMode readMode, WriteMode writeMode) throws Throwable {
        withSparkSession(spark -> {
            String selectSql = String.format("SELECT * FROM %s.%s", DB_NAME, TB_PRIMARY_KEY);

            {
                List<List<Object>> expectedData = new ArrayList<>();
                expectedData.add(Arrays.asList(1001, "google.com", 2));
                expectedData.add(Arrays.asList(1002, "bytedance.com", 4));
                expectedData.add(Arrays.asList(1003, "apache.org", 4));

                String insertSql = String.format("INSERT INTO %s.%s VALUES " +
                                "(1001, 'google.com', 2), " +
                                "(1002, 'bytedance.com', 3), " +
                                "(1003, 'apache.org', 4), " +
                                "(1002, 'bytedance.com', 4)",
                        DB_NAME, TB_PRIMARY_KEY);
                spark.sql(insertSql);

                Dataset<Row> df = spark.sql(selectSql);
                df.show();
                verifyRows(expectedData, df.collectAsList());

                String countSql = String.format("select count(*) from %s.%s", DB_NAME, TB_PRIMARY_KEY);
                df = spark.sql(countSql);
                df.show();
                List<List<Object>> expectedCnt = new ArrayList<>();
                expectedCnt.add(Collections.singletonList(expectedData.size()));
                verifyRows(expectedCnt, df.collectAsList());
            }

            // primary key overwrite key
            {
                List<List<Object>> expectedData = new ArrayList<>();
                expectedData.add(Arrays.asList(1001, "google.com", 2));
                expectedData.add(Arrays.asList(1002, "bytedance.com", 5));
                expectedData.add(Arrays.asList(1003, "apache.org", 4));

                String insertSql = String.format("INSERT INTO %s.%s VALUES (1002, 'bytedance.com', 5)",
                        DB_NAME, TB_PRIMARY_KEY);
                spark.sql(insertSql);

                Dataset<Row> df = spark.sql(selectSql);
                df.show();
                verifyRows(expectedData, df.collectAsList());
            }
        }, useCatalog, readMode, writeMode);
    }

    @ParameterizedTest
    @MethodSource("initMutableParams")
    public void testDataTypes(boolean useCatalog, ReadMode readMode, WriteMode writeMode) throws Throwable {
        String randomString = RandomStringUtils.randomAlphabetic(16);
        withSparkSession(spark -> {
            ActionLog actionLog =
                    new ActionLog(1703128451L, 1002L, "REGISTER", "FAILURE", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)");
            List<ActionLog> actionLogs = Lists.newArrayList(
                    new ActionLog(1703128450L, 1001L, "LOGIN", "SUCCESS"),
                    new ActionLog(1703128451L, 1002L, "REGISTER", "FAILURE")
            );

            List<List<Object>> expectedData = new ArrayList<>();
            expectedData.add(Arrays.asList(
                    false,
                    true,
                    null,
                    127,
                    null,
                    32767,
                    null,
                    2147483647,
                    null,
                    9223372036854775807L,
                    null,
                    "-170141183460469231731687303715884105727",
                    "170141183460469231731687303715884105727",
                    null,
                    3.14,
                    null,
                    3.1415926,
                    null,
                    new BigDecimal("3.1415926560"),
                    new BigDecimal("21.6383780000"),
                    new BigDecimal("4873.629305"),
                    null,
                    randomString,
                    randomString,
                    null,
                    randomString,
                    randomString,
                    null,
                    randomString,
                    null,
                    "2023-02-01",
                    null,
                    "2023-02-01 01:12:01",
                    null,
                    "{\"act_time\": 1703128451, \"act_type\": \"REGISTER\", \"status\": \"FAILURE\", \"user_agent\": \"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)\", \"user_id\": 1002}",
                    "[{\"act_time\": 1703128450, \"act_type\": \"LOGIN\", \"status\": \"SUCCESS\", \"user_id\": 1001}, {\"act_time\": 1703128451, \"act_type\": \"REGISTER\", \"status\": \"FAILURE\", \"user_id\": 1002}]",
                    null));
            String insertSql = String.format("INSERT INTO %s.%s SELECT " +
                            "false, " +
                            "true, " +
                            "null, " +
                            "127, " +
                            "null, " +
                            "32767, " +
                            "null, " +
                            "2147483647, " +
                            "null, " +
                            "9223372036854775807, " +
                            "null, " +
                            "\"-170141183460469231731687303715884105727\", " +
                            "\"170141183460469231731687303715884105727\", " +
                            "null, " +
                            "3.14, " +
                            "null, " +
                            "3.1415926, " +
                            "null, " +
                            "CAST(3.141592656 AS DECIMAL(20, 10)), " +
                            "CAST(21.638378 AS DECIMAL(32, 10)), " +
                            "CAST(4873.6293048479 AS DECIMAL(10, 6)), " +
                            "null, " +
                            "\"" + randomString + "\", " +
                            "\"" + randomString + "\", " +
                            "null, " +
                            "\"" + randomString + "\", " +
                            "\"" + randomString + "\", " +
                            "null, " +
                            "\"" + randomString + "\", " +
                            "null, " +
                            "to_date('2023-02-01', 'yyyy-MM-dd'), " +
                            "null, " +
                            "to_timestamp('2023-02-01 01:12:01'), " +
                            "null, " +
                            "'" + JSON.writeValueAsString(actionLog) + "', " +
                            "'" + JSON.writeValueAsString(actionLogs) + "', " +
                            "null",
                    DB_NAME, TB_DATA_TYPES);
            System.out.println(insertSql);
            spark.sql(insertSql);

            verifyResult(expectedData, scanTable(DB_CONNECTION, DB_NAME, TB_DATA_TYPES));

            Dataset<Row> df = spark.sql(String.format("SELECT * FROM %s.%s", DB_NAME, TB_DATA_TYPES));
            df.show();
            verifyRows(expectedData, df.collectAsList());

            df = spark.sql(String.format("SELECT * FROM %s.%s WHERE int_value=2147483647", DB_NAME, TB_DATA_TYPES));
            df.show();
            verifyRows(expectedData, df.collectAsList());

            df = spark.sql(String.format("SELECT * FROM %s.%s WHERE date_value='2023-02-01'", DB_NAME, TB_DATA_TYPES));
            df.show();
            verifyRows(expectedData, df.collectAsList());

            df = spark.sql(String.format(
                    "SELECT * FROM %s.%s WHERE datetime_value='2023-02-01 01:12:01'", DB_NAME, TB_DATA_TYPES));
            df.show();
            verifyRows(expectedData, df.collectAsList());

            /* json object */

            df = spark.sql(String.format(
                    "SELECT * FROM %s.%s WHERE get_json_object(json_object_value, '$.user_id')=1002", DB_NAME, TB_DATA_TYPES));
            df.show();
            verifyRows(expectedData, df.collectAsList());

            df = spark.sql(String.format(
                    "SELECT * FROM %s.%s " +
                            "WHERE get_json_object(json_object_value, '$.act_type')='REGISTER' " +
                            "AND get_json_object(json_object_value, '$.status')='SUCCESS'", DB_NAME, TB_DATA_TYPES));
            df.show();
            assertTrue(df.collectAsList().isEmpty());

            /* json array */

            df = spark.sql(String.format(
                    "SELECT * FROM %s.%s WHERE get_json_object(json_array_value, '$[0].user_id')=1001", DB_NAME, TB_DATA_TYPES));
            df.show();
            verifyRows(expectedData, df.collectAsList());

            df = spark.sql(String.format(
                    "SELECT * FROM %s.%s " +
                            "WHERE get_json_object(json_array_value, '$[1].act_type')='REGISTER' " +
                            "AND get_json_object(json_array_value, '$[1].status')='SUCCESS'", DB_NAME, TB_DATA_TYPES));
            df.show();
            assertTrue(df.collectAsList().isEmpty());

        }, useCatalog, readMode, writeMode);
    }

    @Test
    void testBypassSpecificDataTypes() throws Throwable {
        String randomString = RandomStringUtils.randomAlphabetic(16);
        String base64RandomString = Base64.encodeBase64String(randomString.getBytes());
        withSparkSession(spark -> {
            List<List<Object>> expectedData = new ArrayList<>();
            expectedData.add(Arrays.asList(
                    1001L,
                    null,
                    randomString,
                    null,
                    randomString));
            String insertSql = String.format("INSERT INTO %s.%s SELECT " +
                            "1001, " +
                            "null, " +
                            "to_binary('" + base64RandomString + "', 'base64'), " +
                            "null, " +
                            "to_binary('" + base64RandomString + "', 'base64')",
                    DB_NAME, TB_SPECIFIC_DATA_TYPES);
            System.out.println(insertSql);
            spark.sql(insertSql);

            verifyResult(expectedData, scanTable(DB_CONNECTION, DB_NAME, TB_SPECIFIC_DATA_TYPES));

            Dataset<Row> df = spark.sql(String.format(
                    "SELECT key_value, " +
                            "CAST(binary_null_value AS STRING), " +
                            "CAST(binary_value AS STRING), " +
                            "CAST(varbinary_null_value AS STRING), " +
                            "CAST(varbinary_value AS STRING) " +
                            "FROM %s.%s", DB_NAME, TB_SPECIFIC_DATA_TYPES));
            df.show();
            verifyRows(expectedData, df.collectAsList());

            df = spark.sql(String.format("SELECT key_value, " +
                    "CAST(binary_null_value AS STRING), " +
                    "CAST(binary_value AS STRING), " +
                    "CAST(varbinary_null_value AS STRING), " +
                    "CAST(varbinary_value AS STRING) " +
                    "FROM %s.%s WHERE key_value=1001", DB_NAME, TB_SPECIFIC_DATA_TYPES));
            df.show();
            verifyRows(expectedData, df.collectAsList());
        }, true, ReadMode.BYPASS, WriteMode.BYPASS);
    }

    @Test
    @Disabled
    public void testNonStarRocksTable() throws Throwable {
        withSparkSession(() -> SparkSession
                        .builder()
                        .master("local[1]")
                        .appName("testNonStarRocksTable")
                        .config("spark.sql.codegen.wholeStage", "false")
                        .config("spark.sql.codegen.factoryMode", "NO_CODEGEN")
                        .enableHiveSupport(),
                spark -> {
                    spark.sql("CREATE TABLE IF NOT EXISTS t1 (date_1 date)");
                    spark.sql("INSERT INTO t1 SELECT to_date('2023-02-01','yyyy-MM-dd') ").show();
                    spark.sql("SELECT * FROM t1").show();
                });
    }

    @Test
    @Disabled("not ready")
    public void testSimplePartitionTable() throws Throwable {
        withSparkSession(spark -> {
            List<List<Object>> expectedData = new ArrayList<>();
            expectedData.add(Arrays.asList(1, 1, "Beijing"));
            expectedData.add(Arrays.asList(2, 2, "London"));
            expectedData.add(Arrays.asList(3, 3, "New York"));

            String insertSql = String.format(
                    "INSERT INTO %s.%s VALUES (1, 1, 'Beijing'), (2, 2, 'London'), (3, 3, 'New York')",
                    DB_NAME, TB_SIMPLE_PARTITION);
            spark.sql(insertSql).show();

            String selectSql = String.format("select * from %s.%s", DB_NAME, TB_SIMPLE_PARTITION);
            spark.sql(selectSql).show();

            List<List<Object>> actualWriteData = scanTable(DB_CONNECTION, DB_NAME, TB_SIMPLE_PARTITION);
            verifyResult(expectedData, actualWriteData);

            String showPartitionsSql = String.format("SHOW PARTITIONS from %s.%s", DB_NAME, TB_SIMPLE_PARTITION);
            spark.sql(showPartitionsSql).show();
        });
    }

    @ParameterizedTest
    @MethodSource("initMutableParams")
    public void testRangePartitionTable(boolean useCatalog, ReadMode readMode, WriteMode writeMode) throws Throwable {
        withSparkSession(spark -> {
            List<List<Object>> expectedData = new ArrayList<>();
            expectedData.add(Arrays.asList(1707463937, 1003, "LOGOUT", "SUCCESS"));
            expectedData.add(Arrays.asList(1710747137, 1004, "LOGIN", "SUCCESS"));
            expectedData.add(Arrays.asList(1712302337, 1005, "REGISTER", "SUCCESS"));

            String insertSql = String.format("INSERT INTO %s.%s VALUES " +
                            // 2023-12-31
                            "(1704007937, 1001, 'LOGIN', 'SUCCESS'), " +
                            // 2023-01-31
                            "(1706686337, 1002, 'LOGIN', 'FAILURE'), " +
                            // 2024-02-09
                            "(1707463937, 1003, 'LOGOUT', 'SUCCESS'), " +
                            // 2024-03-18
                            "(1710747137, 1004, 'LOGIN', 'SUCCESS'), " +
                            // 2024-04-05
                            "(1712302337, 1005, 'REGISTER', 'SUCCESS')",
                    DB_NAME, TB_RANGE_PARTITION);
            spark.sql(insertSql);

            Dataset<Row> df = spark.sql(
                    // 2024-02-01
                    String.format("SELECT * FROM %s.%s WHERE act_time >= 1706716800", DB_NAME, TB_RANGE_PARTITION));
            df.show();
            // 2 + 2 + 2
            assertEquals(6, df.rdd().getNumPartitions());
            verifyRows(expectedData, df.collectAsList());
        }, useCatalog, readMode, writeMode);
    }

    @ParameterizedTest
    @MethodSource("initMutableParams")
    public void testListPartitionTable(boolean useCatalog, ReadMode readMode, WriteMode writeMode) throws Throwable {
        withSparkSession(spark -> {
            List<List<Object>> expectedData = new ArrayList<>();
            expectedData.add(Arrays.asList(1001, 2, 1710747137L));
            expectedData.add(Arrays.asList(1002, 1, 1706686337L));
            expectedData.add(Arrays.asList(1002, 1, 1712302337L));

            String insertSql = String.format("INSERT INTO %s.%s VALUES " +
                            "(1001, 1, 'shanghai', 1704007937), " +
                            "(1002, 1, 'guangdong', 1706686337), " +
                            "(1003, 2, 'xian', 1707463937), " +
                            "(1001, 2, 'beijing', 1710747137), " +
                            "(1002, 1, 'zhejiang', 1712302337)",
                    DB_NAME, TB_LIST_PARTITION);
            executeSrSQL(insertSql);
            // spark.sql(insertSql);

            Dataset<Row> df = spark.sql(String.format(
                    "SELECT user_id, shop_id, access_time " +
                            "FROM %s.%s " +
                            "WHERE city IN ('zhejiang', 'guangdong', 'beijing')",
                    DB_NAME, TB_LIST_PARTITION));
            df.show();
            verifyRows(expectedData, df.collectAsList());
        }, useCatalog, readMode, writeMode);
    }

    @ParameterizedTest
    @MethodSource("initMutableParams")
    public void testExprPartitionTable(boolean useCatalog, ReadMode readMode, WriteMode writeMode) throws Throwable {
        withSparkSession(spark -> {
            String insertSql = String.format("INSERT INTO %s.%s VALUES " +
                            "('zhejiang, hangzhou', 1001, '2024-06-01 14:37:03', 1), " +
                            "('beijing, haidian', 1002, '2024-06-01 14:37:03', 1), " +
                            "('henan, zhengzhou', 1003, '2024-06-01 14:37:03', 1), " +
                            "('zhejiang, hangzhou', 1001, '2024-06-01 14:37:03', 2), " +
                            "('beijing, haidian', 1002, '2024-06-01 14:37:03', 2)",
                    DB_NAME, TB_EXPR_PARTITION);
            executeSrSQL(insertSql);
            // spark.sql(insertSql);

            {
                List<List<Object>> expectedData = new ArrayList<>();
                expectedData.add(Arrays.asList(1001, "zhejiang, hangzhou", 2));
                expectedData.add(Arrays.asList(1002, "beijing, haidian", 2));

                Dataset<Row> df = spark.sql(String.format(
                        "SELECT user_id, user_city, access_cnt " +
                                "FROM %s.%s WHERE access_cnt > 1 AND access_time > '2024-06-01 14:00:00'",
                        DB_NAME, TB_EXPR_PARTITION));
                df.show();
                verifyRows(expectedData, df.collectAsList());
            }

            {
                List<List<Object>> expectedData = new ArrayList<>();
                expectedData.add(Arrays.asList(1001, 1001, "zhejiang, hangzhou"));
                expectedData.add(Arrays.asList(1002, 1002, "beijing, haidian"));

                Dataset<Row> df = spark.sql(String.format(
                        "SELECT user_id, user_id, user_city " +
                                "FROM %s.%s " +
                                "WHERE access_cnt > 1 AND access_time > '2024-06-01 14:00:00'",
                        DB_NAME, TB_EXPR_PARTITION));
                df.show();
                verifyRows(expectedData, df.collectAsList());
            }

            {
                List<List<Object>> expectedData = new ArrayList<>();
                expectedData.add(Arrays.asList("zhejiang, hangzhou", 1001, "2024-06-01 14:37:03", 2));
                expectedData.add(Arrays.asList("beijing, haidian", 1002, "2024-06-01 14:37:03", 2));

                Dataset<Row> df = spark.sql(String.format(
                        "SELECT * " +
                                "FROM %s.%s " +
                                "WHERE access_cnt > 1 AND access_time > '2024-06-01 14:00:00'",
                        DB_NAME, TB_EXPR_PARTITION));
                df.show();
                verifyRows(expectedData, df.collectAsList());
            }
        }, useCatalog, readMode, writeMode);
    }

    @ParameterizedTest
    @MethodSource("initMutableParams")
    public void testFilterPushDown(boolean useCatalog, ReadMode readMode, WriteMode writeMode) throws Throwable {
        withSparkSession(spark -> {
            String insertSql = String.format(
                    "INSERT INTO %s.%s VALUES (1, 'zhangsan', 98), (2, 'lisi', 96), (3, 'wanger', 100)",
                    DB_NAME, TB_FILTER_PUSHDOWN);
            spark.sql(insertSql);

            {
                resetFilterClauseForTest();
                String selectSql = String.format("SELECT name, score FROM %s.%s", DB_NAME, TB_FILTER_PUSHDOWN);
                Dataset<Row> dataFrame = spark.sql(selectSql);
                assertEquals(2, dataFrame.rdd().getNumPartitions());
                assertEquals("", filterClauseForTest());

                List<List<Object>> expectedData = new ArrayList<>();
                expectedData.add(Arrays.asList("zhangsan", 98));
                expectedData.add(Arrays.asList("lisi", 96));
                expectedData.add(Arrays.asList("wanger", 100));
                verifyRows(expectedData, dataFrame.collectAsList());
            }

            {
                resetFilterClauseForTest();
                String selectSql = String.format("SELECT name, score FROM %s.%s WHERE id > 1", DB_NAME, TB_FILTER_PUSHDOWN);
                Dataset<Row> dataFrame = spark.sql(selectSql);
                assertEquals(2, dataFrame.rdd().getNumPartitions());
                assertEquals("(`id` is not null) and (`id` > 1)", filterClauseForTest());

                List<List<Object>> expectedData = new ArrayList<>();
                expectedData.add(Arrays.asList("lisi", 96));
                expectedData.add(Arrays.asList("wanger", 100));
                verifyRows(expectedData, dataFrame.collectAsList());
            }

            {
                resetFilterClauseForTest();
                String selectSql = String.format("SELECT name, score FROM %s.%s WHERE id = 2", DB_NAME, TB_FILTER_PUSHDOWN);
                Dataset<Row> dataFrame = spark.sql(selectSql);
                assertEquals(1, dataFrame.rdd().getNumPartitions());
                assertEquals("(`id` is not null) and (`id` = 2)", filterClauseForTest());

                List<List<Object>> expectedData = new ArrayList<>();
                expectedData.add(Arrays.asList("lisi", 96));
                verifyRows(expectedData, dataFrame.collectAsList());
            }
        }, useCatalog, readMode, writeMode);
    }

    @Test
    public void testFilterPushDownWithCustomFiltersForRpcRead() throws Throwable {
        withSparkSession(
                builder -> builder.config("spark.sql.catalog.starrocks.filter.query", "score > 96"),
                spark -> {
                    String insertSql = String.format(
                            "INSERT INTO %s.%s VALUES (1, 'zhangsan', 98), (2, 'lisi', 96), (3, 'wanger', 100)",
                            DB_NAME, TB_FILTER_PUSHDOWN);
                    spark.sql(insertSql);

                    {
                        resetFilterClauseForTest();
                        String selectSql = String.format("SELECT name, score FROM %s.%s", DB_NAME, TB_FILTER_PUSHDOWN);
                        Dataset<Row> dataFrame = spark.sql(selectSql);
                        assertEquals(2, dataFrame.rdd().getNumPartitions());
                        assertEquals("score > 96", filterClauseForTest());

                        List<List<Object>> expectedData = new ArrayList<>();
                        expectedData.add(Arrays.asList("zhangsan", 98));
                        // expectedData.add(Arrays.asList("lisi", 96));
                        expectedData.add(Arrays.asList("wanger", 100));
                        verifyRows(expectedData, dataFrame.collectAsList());
                    }

                    {
                        resetFilterClauseForTest();
                        String selectSql = String.format(
                                "SELECT name, score FROM %s.%s WHERE id > 1", DB_NAME, TB_FILTER_PUSHDOWN);
                        Dataset<Row> dataFrame = spark.sql(selectSql);
                        assertEquals(2, dataFrame.rdd().getNumPartitions());
                        assertEquals("(`id` is not null) and (`id` > 1) and (score > 96)", filterClauseForTest());

                        List<List<Object>> expectedData = new ArrayList<>();
                        // expectedData.add(Arrays.asList("lisi", 96));
                        expectedData.add(Arrays.asList("wanger", 100));
                        verifyRows(expectedData, dataFrame.collectAsList());
                    }
                }, true, ReadMode.RPC, null);
    }

    @Test
    @Disabled
    public void testFilterPushDownWithCustomFiltersForBypassRead() throws Throwable {
        withSparkSession(
                builder -> builder.config("spark.sql.catalog.starrocks.filter.query", "score > 96"),
                spark -> {
                    String insertSql = String.format(
                            "INSERT INTO %s.%s VALUES (1, 'zhangsan', 98), (2, 'lisi', 96), (3, 'wanger', 100)",
                            DB_NAME, TB_FILTER_PUSHDOWN);
                    spark.sql(insertSql);

                    {
                        resetFilterClauseForTest();
                        String selectSql = String.format("SELECT name, score FROM %s.%s", DB_NAME, TB_FILTER_PUSHDOWN);
                        Dataset<Row> dataFrame = spark.sql(selectSql);
                        assertEquals(2, dataFrame.rdd().getNumPartitions());
                        assertEquals("", filterClauseForTest());

                        List<List<Object>> expectedData = new ArrayList<>();
                        expectedData.add(Arrays.asList("zhangsan", 98));
                        expectedData.add(Arrays.asList("lisi", 96));
                        expectedData.add(Arrays.asList("wanger", 100));
                        verifyRows(expectedData, dataFrame.collectAsList());
                    }

                    {
                        resetFilterClauseForTest();
                        String selectSql = String.format(
                                "SELECT name, score FROM %s.%s WHERE id > 1", DB_NAME, TB_FILTER_PUSHDOWN);
                        Dataset<Row> dataFrame = spark.sql(selectSql);
                        assertEquals(2, dataFrame.rdd().getNumPartitions());
                        assertEquals("(`id` is not null) and (`id` > 1)", filterClauseForTest());

                        List<List<Object>> expectedData = new ArrayList<>();
                        expectedData.add(Arrays.asList("lisi", 96));
                        expectedData.add(Arrays.asList("wanger", 100));
                        verifyRows(expectedData, dataFrame.collectAsList());
                    }
                }, true, ReadMode.BYPASS, null);
    }

    @ParameterizedTest
    @MethodSource("initMutableParams")
    public void testFilterPushDownWithJoin(boolean useCatalog, ReadMode readMode, WriteMode writeMode) throws Throwable {
        withSparkSession(spark -> {
            long timestamp = System.currentTimeMillis();

            {
                String insertSql = String.format(
                        "INSERT INTO %s.%s VALUES " +
                                "(" + timestamp + ", 1001, 'LOGIN', 'SUCCESS', ''), " +
                                "(" + timestamp +
                                ", 1002, 'REGISTER', 'FAILURE', 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)'), " +
                                "(" + timestamp + ", 1003, 'LOGOUT', 'SUCCESS', '')",
                        DB_NAME, TB_DUPLICATE_KEY);
                spark.sql(insertSql);

                String selectSql = String.format("SELECT * FROM %s.%s", DB_NAME, TB_DUPLICATE_KEY);
                Dataset<Row> df = spark.sql(selectSql);
                df.show();
            }

            {
                String insertSql = String.format("INSERT INTO %s.%s VALUES " +
                                "(1001, 'google.com', 2), " +
                                "(1002, 'bytedance.com', 3), " +
                                "(1003, 'apache.org', 4), " +
                                "(1002, 'bytedance.com', 4)",
                        DB_NAME, TB_PRIMARY_KEY);
                spark.sql(insertSql);

                String selectSql = String.format("SELECT * FROM %s.%s", DB_NAME, TB_PRIMARY_KEY);
                Dataset<Row> df = spark.sql(selectSql);
                df.show();
            }

            List<List<Object>> expectedData = new ArrayList<>();
            expectedData.add(Arrays.asList(
                    timestamp, 1002L, "REGISTER", "FAILURE", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
                    1002, "bytedance.com", 4));

            String joinSelectSql = "SELECT * " +
                    "FROM " + DB_NAME + ".tb_duplicate_key dk " +
                    "         JOIN " + DB_NAME + ".tb_primary_key pk ON dk.user_id = pk.user_id " +
                    "WHERE dk.act_type = 'REGISTER' AND pk.pv > 0";

            Dataset<Row> df = spark.sql(joinSelectSql);
            df.show();
            verifyRows(expectedData, df.collectAsList());

        }, useCatalog, readMode, writeMode);
    }

    @ParameterizedTest
    @MethodSource("initMutableParams")
    public void testReadInConcurrency(boolean useCatalog, ReadMode readMode, WriteMode writeMode) throws Throwable {
        withSparkSession(spark -> {
            long timestamp = System.currentTimeMillis();

            {
                String insertSql = String.format(
                        "INSERT INTO %s.%s VALUES " +
                                "(" + timestamp + ", 1001, 'LOGIN', 'SUCCESS', ''), " +
                                "(" + timestamp +
                                ", 1002, 'REGISTER', 'FAILURE', 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)'), " +
                                "(" + timestamp + ", 1003, 'LOGOUT', 'SUCCESS', '')",
                        DB_NAME, TB_DUPLICATE_KEY);
                spark.sql(insertSql);

                String selectSql = String.format("SELECT * FROM %s.%s", DB_NAME, TB_DUPLICATE_KEY);
                Dataset<Row> df = spark.sql(selectSql);
                df.show();
            }

            {
                String insertSql = String.format("INSERT INTO %s.%s VALUES " +
                                "(1001, 'google.com', 2), " +
                                "(1002, 'bytedance.com', 3), " +
                                "(1003, 'apache.org', 4), " +
                                "(1002, 'bytedance.com', 4)",
                        DB_NAME, TB_PRIMARY_KEY);
                spark.sql(insertSql);

                String selectSql = String.format("SELECT * FROM %s.%s", DB_NAME, TB_PRIMARY_KEY);
                Dataset<Row> df = spark.sql(selectSql);
                df.show();
            }

            List<List<Object>> expectedData = new ArrayList<>();
            expectedData.add(Arrays.asList(
                    timestamp, 1002L, "REGISTER", "FAILURE", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
                    1002, "bytedance.com", 4));

            String joinSelectSql = "SELECT * " +
                    "FROM " + DB_NAME + ".tb_duplicate_key dk " +
                    "         JOIN " + DB_NAME + ".tb_primary_key pk ON dk.user_id = pk.user_id " +
                    "WHERE dk.act_type = 'REGISTER' AND pk.pv > 0";

            ExecutorService es = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
            List<Callable<Boolean>> tasks = new ArrayList<>();
            IntStream.range(0, 100).forEach(i ->
                    tasks.add(() -> {
                        System.out.println("Thread " + Thread.currentThread().getId() + " processing " + i);
                        Dataset<Row> df = spark.sql(joinSelectSql);
                        df.show();
                        verifyRows(expectedData, df.collectAsList());
                        return true;
                    }));

            List<Future<Boolean>> futures = es.invokeAll(tasks);
            for (Future<Boolean> future : futures) {
                assertTrue(future.get());
            }

            es.shutdownNow();

        }, useCatalog, readMode, writeMode);
    }

    @ParameterizedTest
    @MethodSource("initMutableParams")
    public void testRollbackTransaction(boolean useCatalog, ReadMode readMode, WriteMode writeMode) throws Throwable {
        String txnLabel = RandomStringUtils.randomAlphabetic(32).toLowerCase();
        withSparkSession(builder -> {
                    builder.config("spark.sql.catalog.starrocks.write.transaction.label", txnLabel);
                    return builder;
                },
                spark -> {
                    List<List<Object>> expected = new ArrayList<>();
                    for (int i = 0; i < 10; i++) {
                        expected.add(Arrays.asList(
                                1001L + i,
                                System.currentTimeMillis(),
                                RandomUtils.nextInt(1, 10),
                                "'10.160.72." + i + "'",
                                "\"170141183460469231731687303715884105727\"",
                                "'Test Rollback Transaction'"));
                    }

                    // add bad data
                    expected.add(Arrays.asList(
                            2001L,
                            System.currentTimeMillis(),
                            RandomUtils.nextInt(1, 10),
                            "'127.0.0.1'",
                            "\"++1\"",
                            "'Bad Data'"));
                    try {
                        String insertSql = String.format(
                                "INSERT INTO %s.%s VALUES " + expected.stream()
                                        .map(objects -> objects.stream()
                                                .map(Object::toString)
                                                .collect(Collectors.joining(", ", "(", ")")))
                                        .collect(Collectors.joining(", ")),
                                DB_NAME, TB_TRANSACTION);
                        spark.sql(insertSql);
                        fail();
                    } catch (Throwable t) {
                        t.printStackTrace();
                    }

                    // assertEquals(TransactionStatus.ABORTED, getTransactionStatus(DB_NAME, TB_TRANSACTION, txnLabel));
                    Dataset<Row> dataFrame = spark.sql(String.format("SELECT * FROM %s.%s", DB_NAME, TB_TRANSACTION));
                    assertEquals(0, dataFrame.collectAsList().size());
                }, useCatalog, readMode, WriteMode.BYPASS);
    }

    @Test
    @Disabled
    public void testQ10() throws Throwable {
        String currentSql = "use tpcds_1t;";
        String sql = "select\n" +
                "  cd_gender,\n" +
                "  cd_marital_status,\n" +
                "  cd_education_status,\n" +
                "  count(*) cnt1,\n" +
                "  cd_purchase_estimate,\n" +
                "  count(*) cnt2,\n" +
                "  cd_credit_rating,\n" +
                "  count(*) cnt3,\n" +
                "  cd_dep_count,\n" +
                "  count(*) cnt4,\n" +
                "  cd_dep_employed_count,\n" +
                "  count(*) cnt5,\n" +
                "  cd_dep_college_count,\n" +
                "  count(*) cnt6\n" +
                " from\n" +
                "  customer c,customer_address ca,customer_demographics\n" +
                " where\n" +
                "  c.c_current_addr_sk = ca.ca_address_sk and\n" +
                "  ca_county in ('Rush County','Toole County','Jefferson County','Dona Ana County','La Porte County') and\n" +
                "  cd_demo_sk = c.c_current_cdemo_sk and\n" +
                "  exists (select *\n" +
                "          from store_sales,date_dim\n" +
                "          where c.c_customer_sk = ss_customer_sk and\n" +
                "                ss_sold_date_sk = d_date_sk and\n" +
                "                d_year = 2002 and\n" +
                "                d_moy between 1 and 1+3) and\n" +
                "   (exists (select *\n" +
                "            from web_sales,date_dim\n" +
                "            where c.c_customer_sk = ws_bill_customer_sk and\n" +
                "                  ws_sold_date_sk = d_date_sk and\n" +
                "                  d_year = 2002 and\n" +
                "                  d_moy between 1 ANd 1+3) or\n" +
                "    exists (select *\n" +
                "            from catalog_sales,date_dim\n" +
                "            where c.c_customer_sk = cs_ship_customer_sk and\n" +
                "                  cs_sold_date_sk = d_date_sk and\n" +
                "                  d_year = 2002 and\n" +
                "                  d_moy between 1 and 1+3))\n" +
                " group by cd_gender,\n" +
                "          cd_marital_status,\n" +
                "          cd_education_status,\n" +
                "          cd_purchase_estimate,\n" +
                "          cd_credit_rating,\n" +
                "          cd_dep_count,\n" +
                "          cd_dep_employed_count,\n" +
                "          cd_dep_college_count\n" +
                " order by cd_gender,\n" +
                "          cd_marital_status,\n" +
                "          cd_education_status,\n" +
                "          cd_purchase_estimate,\n" +
                "          cd_credit_rating,\n" +
                "          cd_dep_count,\n" +
                "          cd_dep_employed_count,\n" +
                "          cd_dep_college_count\n" +
                "limit 100;\n";
        withSparkSession(spark -> {
            spark.sql(currentSql).show();
            spark.sql(sql).show();
        }, true, ReadMode.BYPASS, WriteMode.BYPASS);

    }

    @Test
    @Disabled
    public void testExternalQ10() throws Throwable {
        String customerAdd = "CREATE TABLE IF NOT EXISTS `customer_address`\n" +
                "USING starrocks\n" +
                "OPTIONS(\n" +
                "   \"starrocks.fe.http.url\"=\"218.30.103.23:8030\",\n" +
                "   \"starrocks.fe.jdbc.url\"=\"jdbc:mysql://218.30.103.23:9030\",\n" +
                "   \"starrocks.table.identifier\"=\"tpcds_1t.customer_address\",\n" +
                "   \"starrocks.user\"=\"root\",\n" +
                "   \"starrocks.password\"=\"password1A!\",\n" +
                "   \"starrocks.request.tablet.size\"=\"1\",\n" +
                "   \"starrocks.batch.size\"=\"8092\"\n" +
                ");";
        String customer = "CREATE TABLE  IF NOT EXISTS  `customer`\n" +
                "USING starrocks\n" +
                "OPTIONS(\n" +
                "   \"starrocks.fe.http.url\"=\"218.30.103.23:8030\",\n" +
                "   \"starrocks.fe.jdbc.url\"=\"jdbc:mysql://218.30.103.23:9030\",\n" +
                "   \"starrocks.table.identifier\"=\"tpcds_1t.customer\",\n" +
                "   \"starrocks.user\"=\"root\",\n" +
                "   \"starrocks.password\"=\"password1A!\",\n" +
                "   \"starrocks.request.tablet.size\"=\"1\",\n" +
                "   \"starrocks.batch.size\"=\"8092\"\n" +
                ");";
        String customer_demographics = "CREATE TABLE IF NOT EXISTS `customer_demographics`\n" +
                "USING starrocks\n" +
                "OPTIONS(\n" +
                "   \"starrocks.fe.http.url\"=\"218.30.103.23:8030\",\n" +
                "   \"starrocks.fe.jdbc.url\"=\"jdbc:mysql://218.30.103.23:9030\",\n" +
                "   \"starrocks.table.identifier\"=\"tpcds_1t.customer_demographics\",\n" +
                "   \"starrocks.user\"=\"root\",\n" +
                "   \"starrocks.password\"=\"password1A!\",\n" +
                "   \"starrocks.request.tablet.size\"=\"1\",\n" +
                "   \"starrocks.batch.size\"=\"8092\"\n" +
                ");";
        String store_sales = "CREATE TABLE  IF NOT EXISTS `store_sales`\n" +
                "USING starrocks\n" +
                "OPTIONS(\n" +
                "   \"starrocks.fe.http.url\"=\"218.30.103.23:8030\",\n" +
                "   \"starrocks.fe.jdbc.url\"=\"jdbc:mysql://218.30.103.23:9030\",\n" +
                "   \"starrocks.table.identifier\"=\"tpcds_1t.store_sales\",\n" +
                "   \"starrocks.user\"=\"root\",\n" +
                "   \"starrocks.password\"=\"password1A!\",\n" +
                "   \"starrocks.request.tablet.size\"=\"1\",\n" +
                "   \"starrocks.batch.size\"=\"8092\"\n" +
                ");";
        String date_dim = "\n" +
                "CREATE TABLE IF NOT EXISTS `date_dim`\n" +
                "USING starrocks\n" +
                "OPTIONS(\n" +
                "   \"starrocks.fe.http.url\"=\"218.30.103.23:8030\",\n" +
                "   \"starrocks.fe.jdbc.url\"=\"jdbc:mysql://218.30.103.23:9030\",\n" +
                "   \"starrocks.table.identifier\"=\"tpcds_1t.date_dim\",\n" +
                "   \"starrocks.user\"=\"root\",\n" +
                "   \"starrocks.password\"=\"password1A!\",\n" +
                "   \"starrocks.request.tablet.size\"=\"1\",\n" +
                "   \"starrocks.batch.size\"=\"8092\"\n" +
                ");";
        String web_sales = "CREATE TABLE IF NOT EXISTS `web_sales`\n" +
                "USING starrocks\n" +
                "OPTIONS(\n" +
                "   \"starrocks.fe.http.url\"=\"218.30.103.23:8030\",\n" +
                "   \"starrocks.fe.jdbc.url\"=\"jdbc:mysql://218.30.103.23:9030\",\n" +
                "   \"starrocks.table.identifier\"=\"tpcds_1t.web_sales\",\n" +
                "   \"starrocks.user\"=\"root\",\n" +
                "   \"starrocks.password\"=\"password1A!\",\n" +
                "   \"starrocks.request.tablet.size\"=\"1\",\n" +
                "   \"starrocks.batch.size\"=\"8092\"\n" +
                ");";

        String catalog_sales = "CREATE TABLE IF NOT EXISTS `catalog_sales`\n" +
                "USING starrocks\n" +
                "OPTIONS(\n" +
                "   \"starrocks.fe.http.url\"=\"218.30.103.23:8030\",\n" +
                "   \"starrocks.fe.jdbc.url\"=\"jdbc:mysql://218.30.103.23:9030\",\n" +
                "   \"starrocks.table.identifier\"=\"tpcds_1t.catalog_sales\",\n" +
                "   \"starrocks.user\"=\"root\",\n" +
                "   \"starrocks.password\"=\"password1A!\",\n" +
                "   \"starrocks.request.tablet.size\"=\"1\",\n" +
                "   \"starrocks.batch.size\"=\"8092\"\n" +
                ");";

        String sql = "select\n" +
                "  cd_gender,\n" +
                "  cd_marital_status,\n" +
                "  cd_education_status,\n" +
                "  count(*) cnt1,\n" +
                "  cd_purchase_estimate,\n" +
                "  count(*) cnt2,\n" +
                "  cd_credit_rating,\n" +
                "  count(*) cnt3,\n" +
                "  cd_dep_count,\n" +
                "  count(*) cnt4,\n" +
                "  cd_dep_employed_count,\n" +
                "  count(*) cnt5,\n" +
                "  cd_dep_college_count,\n" +
                "  count(*) cnt6\n" +
                " from\n" +
                "  customer c,customer_address ca,customer_demographics\n" +
                " where\n" +
                "  c.c_current_addr_sk = ca.ca_address_sk and\n" +
                "  ca_county in ('Rush County','Toole County','Jefferson County','Dona Ana County','La Porte County') and\n" +
                "  cd_demo_sk = c.c_current_cdemo_sk and\n" +
                "  exists (select *\n" +
                "          from store_sales,date_dim\n" +
                "          where c.c_customer_sk = ss_customer_sk and\n" +
                "                ss_sold_date_sk = d_date_sk and\n" +
                "                d_year = 2002 and\n" +
                "                d_moy between 1 and 1+3) and\n" +
                "   (exists (select *\n" +
                "            from web_sales,date_dim\n" +
                "            where c.c_customer_sk = ws_bill_customer_sk and\n" +
                "                  ws_sold_date_sk = d_date_sk and\n" +
                "                  d_year = 2002 and\n" +
                "                  d_moy between 1 ANd 1+3) or\n" +
                "    exists (select *\n" +
                "            from catalog_sales,date_dim\n" +
                "            where c.c_customer_sk = cs_ship_customer_sk and\n" +
                "                  cs_sold_date_sk = d_date_sk and\n" +
                "                  d_year = 2002 and\n" +
                "                  d_moy between 1 and 1+3))\n" +
                " group by cd_gender,\n" +
                "          cd_marital_status,\n" +
                "          cd_education_status,\n" +
                "          cd_purchase_estimate,\n" +
                "          cd_credit_rating,\n" +
                "          cd_dep_count,\n" +
                "          cd_dep_employed_count,\n" +
                "          cd_dep_college_count\n" +
                " order by cd_gender,\n" +
                "          cd_marital_status,\n" +
                "          cd_education_status,\n" +
                "          cd_purchase_estimate,\n" +
                "          cd_credit_rating,\n" +
                "          cd_dep_count,\n" +
                "          cd_dep_employed_count,\n" +
                "          cd_dep_college_count\n" +
                "limit 100;\n";
        withSparkSession(spark -> {
            spark.sql(customerAdd).show();
            spark.sql(customer).show();
            spark.sql(customer_demographics).show();
            spark.sql(store_sales).show();
            spark.sql(date_dim).show();
            spark.sql(web_sales).show();
            spark.sql(catalog_sales).show();
            spark.sql(sql).show();
        }, false, null, null);

    }
}