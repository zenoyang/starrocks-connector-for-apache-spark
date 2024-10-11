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

package com.starrocks.connector.spark.util;

import com.starrocks.connector.spark.sql.conf.WriteStarRocksConfig;
import com.starrocks.connector.spark.sql.write.StarRocksWriterCommitMessage;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class ExecutorResProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(ExecutorResProcessor.class);
    private List<SegmentLoadDqc> dqcList = new ArrayList<>();
    private List<String> dataPathList = new ArrayList<>();
    private final boolean isShareNothingMode;
    private final WriteStarRocksConfig config;

    public ExecutorResProcessor(WriteStarRocksConfig config) {
        this.config = config;
        this.isShareNothingMode = config.isShareNothingBulkLoadEnabled();
    }

    public synchronized void collectExecRes(WriterCommitMessage commitMessage) {
        if (commitMessage instanceof StarRocksWriterCommitMessage) {
            StarRocksWriterCommitMessage msg = (StarRocksWriterCommitMessage) commitMessage;
            dqcList.add(msg.getDqc());
            if (isShareNothingMode) {
                dataPathList.add(msg.getWorkSpacePath());
            }
        } else {
            LOG.error("Not StarRocksWriterCommitMessage");
        }
    }

    public void writeDqcFileToS3() {
        String dqcResJson = SegmentLoadDqc.mergeDqcList(dqcList).toJson();
        StarRocksWriterUtils.writeToS3(dqcResJson,
                config.getShareNothingBulkLoadPath() + "dqc.json", ConfigUtils.getConfiguration(config.getOriginOptions()));
        LOG.info("Dqc result: {}", dqcResJson);
    }

    public void mvDataToResultPath() {
        String resultBasePath = config.getShareNothingBulkLoadPath();
        String currentWorkSpacePath = config.getWorkSpacePath();
        Configuration s3Config = ConfigUtils.getConfiguration(config.getOriginOptions());
        for (String tmpDataPath : dataPathList) {
            String targetPath = computeTargetPath(resultBasePath, currentWorkSpacePath, tmpDataPath);
            StarRocksWriterUtils.moveS3File(tmpDataPath, targetPath, s3Config);
        }
        StarRocksWriterUtils.clearS3File(currentWorkSpacePath, s3Config);
        LOG.info("Remove workspace path: {}", currentWorkSpacePath);
    }

    private String computeTargetPath(String targetBasePath, String currentWorkSpacePath, String currentDataPath) {
        // oss://xxxxx/.work_space/{tableid}/{partitionid}/{indexid}/{uuid} => oss://xxxxx/{tableid}/{partitionid}/{indexid}
        String partOfTabletPath = currentDataPath.replace(currentWorkSpacePath, "");
        partOfTabletPath = partOfTabletPath.substring(0, partOfTabletPath.lastIndexOf('/'));
        return targetBasePath + partOfTabletPath;
    }
}
