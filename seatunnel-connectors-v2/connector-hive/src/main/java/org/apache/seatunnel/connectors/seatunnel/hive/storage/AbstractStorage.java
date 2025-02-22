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

package org.apache.seatunnel.connectors.seatunnel.hive.storage;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigValueFactory;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.file.config.BaseSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.hive.config.HiveConfig;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.ImmutableList;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Slf4j
public abstract class AbstractStorage implements Storage {
    private static final Option BUCKET_OPTION = Options.key("bucket").stringType().noDefaultValue();
    private static final List<String> HADOOP_CONF_FILES =
            ImmutableList.of("core-site.xml", "hdfs-site.xml", "hive-site.xml");

    protected Config fillBucket(ReadonlyConfig readonlyConfig, Configuration configuration) {
        Config config = readonlyConfig.toConfig();
        String bucketValue = configuration.get(BUCKET_OPTION.key());
        if (StringUtils.isBlank(bucketValue)) {
            throw new RuntimeException(
                    "There is no bucket property in conf which load from [hadoop_conf_path,hadoop_conf].");
        }
        config = config.withValue(BUCKET_OPTION.key(), ConfigValueFactory.fromAnyRef(bucketValue));
        return config;
    }

    /**
     * Loading Hadoop configuration by hadoop conf path or props set by hive.hadoop.conf
     *
     * @return
     */
    protected Configuration loadHiveBaseHadoopConfig(ReadonlyConfig readonlyConfig) {
        Configuration configuration = new Configuration();
        // Try to load from hadoop_conf_path(The Bucket configuration is typically in core-site.xml)
        Optional<String> hadoopConfPath = readonlyConfig.getOptional(HiveConfig.HADOOP_CONF_PATH);
        if (hadoopConfPath.isPresent()) {
            HADOOP_CONF_FILES.forEach(
                    confFile -> {
                        java.nio.file.Path path = Paths.get(hadoopConfPath.get(), confFile);
                        if (Files.exists(path)) {
                            try {
                                configuration.addResource(path.toUri().toURL());
                            } catch (IOException e) {
                                log.warn(
                                        "Error adding Hadoop resource {}, resource was not added",
                                        path,
                                        e);
                            }
                        }
                    });
        }
        readonlyConfig
                .getOptional(BaseSinkConfig.HDFS_SITE_PATH)
                .ifPresent(
                        hdfsSitePath -> {
                            try {
                                configuration.addResource(new File(hdfsSitePath).toURI().toURL());
                            } catch (IOException e) {
                                log.warn(
                                        "Error adding Hadoop resource {}, resource was not added",
                                        hdfsSitePath,
                                        e);
                            }
                        });
        readonlyConfig
                .getOptional(HiveConfig.HIVE_SITE_PATH)
                .ifPresent(
                        hiveSitePath -> {
                            try {
                                configuration.addResource(new File(hiveSitePath).toURI().toURL());
                            } catch (IOException e) {
                                log.warn(
                                        "Error adding Hadoop resource {}, resource was not added",
                                        hiveSitePath,
                                        e);
                            }
                        });
        // Try to load from hadoopConf
        Optional<Map<String, String>> hadoopConf =
                readonlyConfig.getOptional(HiveConfig.HADOOP_CONF);
        if (hadoopConf.isPresent()) {
            hadoopConf.get().forEach((k, v) -> configuration.set(k, v));
        }
        return configuration;
    }
}
