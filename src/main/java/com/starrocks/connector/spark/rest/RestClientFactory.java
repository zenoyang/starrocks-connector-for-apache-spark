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

package com.starrocks.connector.spark.rest;


import com.starrocks.connector.spark.sql.conf.StarRocksConfig;
import com.starrocks.format.rest.RestClient;

import java.util.Optional;

public class RestClientFactory {

    /**
     * Create {@link RestClient} instance.
     */
    public static RestClient create(StarRocksConfig config, boolean skipFeConnectCheck) {
        RestClient.Builder builder = new RestClient.Builder()
                .setSkipFeConnectCheck(skipFeConnectCheck);
        if (!skipFeConnectCheck) {
            builder.setFeEndpoints(config.getFeHttpUrls())
                    .setUsername(config.getUsername())
                    .setPassword(config.getPassword());
        }

        Optional.ofNullable(config.getHttpRequestConnectTimeoutMs())
                .ifPresent(builder::setConnectTimeoutMillis);

        Optional.ofNullable(config.getHttpRequestSocketTimeoutMs())
                .ifPresent(builder::setSocketTimeoutMillis);

        Optional.ofNullable(config.getHttpRequestRetries())
                .ifPresent(builder::setRetries);

        return builder.build();
    }

    public static RestClient create(StarRocksConfig config) {
        return create(config, false);
    }
}
