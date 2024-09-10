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

package com.starrocks.connector.spark.util;

import com.starrocks.connector.spark.cfg.Settings;
import com.starrocks.connector.spark.sql.conf.ReadStarRocksConfig.ReadMode;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.hadoop.conf.Configuration;

import java.util.Map;

import static com.starrocks.connector.spark.sql.conf.ReadStarRocksConfig.FILTER_PUSHDOWN_ENABLED;
import static com.starrocks.connector.spark.sql.conf.ReadStarRocksConfig.KEY_READ_MODE;
import static com.starrocks.connector.spark.sql.conf.ReadStarRocksConfig.USE_STARROCKS_CATALOG;
import static com.starrocks.connector.spark.sql.conf.StarRocksConfigBase.KEY_VERBOSE_ENABLED;

public class ConfigUtils {

    public static boolean isVerbose(Settings settings) {
        return BooleanUtils.toBoolean(settings.getProperty(KEY_VERBOSE_ENABLED, Boolean.FALSE.toString()));
    }

    public static boolean isBypassRead(Settings settings) {
        return ReadMode.BYPASS.is(settings.getProperty(KEY_READ_MODE));
    }

    public static boolean notBypassRead(Settings settings) {
        return !isBypassRead(settings);
    }

    public static boolean useStarRocksCatalog(Settings settings) {
        return BooleanUtils.toBoolean(settings.getProperty(USE_STARROCKS_CATALOG, Boolean.FALSE.toString()));
    }

    public static boolean isFilterPushDownEnabled(Settings settings) {
        return BooleanUtils.toBoolean(settings.getProperty(FILTER_PUSHDOWN_ENABLED, Boolean.TRUE.toString()));
    }

    private ConfigUtils() {
    }

    public static Configuration getConfiguration(Map<String, String> originOptions) {
        Configuration configuration = new Configuration();
        configuration.set("fs.oss.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        configuration.set("fs.AbstractFileSystem.s3.impl", "org.apache.hadoop.fs.s3a.S3A");
        configuration.set("fs.s3.endpoint", originOptions.get("starrocks.fs.s3a.endpoint"));
        configuration.set("fs.s3a.endpoint", originOptions.get("starrocks.fs.s3a.endpoint"));
        configuration.set("fs.s3a.path.style.access",
                originOptions.getOrDefault("starrocks.fs.s3a.path.style.access", "true"));
        configuration.set("fs.s3a.connection.maximum", "2000");
        configuration.set("fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
        configuration.set("fs.s3a.access.key", originOptions.get("starrocks.fs.s3a.access.key"));
        configuration.set("fs.s3a.secret.key", originOptions.get("starrocks.fs.s3a.secret.key"));
        return configuration;
    }
}
