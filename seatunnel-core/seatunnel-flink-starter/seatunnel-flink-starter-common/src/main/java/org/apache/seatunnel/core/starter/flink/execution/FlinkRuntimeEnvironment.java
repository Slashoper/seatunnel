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

package org.apache.seatunnel.core.starter.flink.execution;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.PipelineOptionsInternal;
import org.apache.seatunnel.core.starter.flink.args.FlinkCommandArgs;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.env.EnvCommonOptions;
import org.apache.seatunnel.common.Constants;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.common.utils.ReflectionUtils;
import org.apache.seatunnel.core.starter.execution.RuntimeEnvironment;
import org.apache.seatunnel.core.starter.flink.utils.ConfigKeyName;
import org.apache.seatunnel.core.starter.flink.utils.EnvironmentUtil;
import org.apache.seatunnel.core.starter.flink.utils.TableUtil;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.TernaryBoolean;

import lombok.extern.slf4j.Slf4j;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@Slf4j
public class FlinkRuntimeEnvironment implements RuntimeEnvironment {
    private static final String RESULT_TABLE_NAME = "result_table_name";
    private static volatile FlinkRuntimeEnvironment INSTANCE = null;
    private Config config;

    private StreamExecutionEnvironment environment;

    private StreamTableEnvironment tableEnvironment;

    private JobMode jobMode;

    private String jobName = Constants.LOGO;


    private FlinkCommandArgs flinkCommandArgs;

    private FlinkRuntimeEnvironment(Config config,FlinkCommandArgs flinkCommandArgs) {
        this.setFlinkCommandArgs(flinkCommandArgs);
        this.initialize(config);
    }


    public void setFlinkCommandArgs(FlinkCommandArgs flinkCommandArgs) {
        this.flinkCommandArgs = flinkCommandArgs;
    }

    @Override
    public FlinkRuntimeEnvironment setConfig(Config config) {
        this.config = config;
        return this;
    }

    @Override
    public Config getConfig() {
        return config;
    }

    @Override
    public CheckResult checkConfig() {
        return EnvironmentUtil.checkRestartStrategy(config);
    }

    @Override
    public FlinkRuntimeEnvironment prepare() {
        createStreamEnvironment();
        createStreamTableEnvironment();
        if (config.hasPath("job.name")) {
            jobName = config.getString("job.name");
        }
        return this;
    }

    public String getJobName() {
        return jobName;
    }

    public boolean isStreaming() {
        return JobMode.STREAMING.equals(jobMode);
    }

    @Override
    public FlinkRuntimeEnvironment setJobMode(JobMode jobMode) {
        this.jobMode = jobMode;
        return this;
    }

    @Override
    public JobMode getJobMode() {
        return jobMode;
    }

    @Override
    public void registerPlugin(List<URL> pluginPaths) {
        pluginPaths.forEach(url -> log.info("register plugins : {}", url));
        List<Configuration> configurations = new ArrayList<>();
        try {
            configurations.add(
                    (Configuration)
                            Objects.requireNonNull(
                                            ReflectionUtils.getDeclaredMethod(
                                                    StreamExecutionEnvironment.class,
                                                    "getConfiguration"))
                                    .orElseThrow(
                                            () ->
                                                    new RuntimeException(
                                                            "can't find "
                                                                    + "method: getConfiguration"))
                                    .invoke(this.environment));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        configurations.forEach(
                configuration -> {
                    List<String> jars = configuration.get(PipelineOptions.JARS);
                    if (jars == null) {
                        jars = new ArrayList<>();
                    }
                    jars.addAll(
                            pluginPaths.stream().map(URL::toString).collect(Collectors.toList()));
                    configuration.set(
                            PipelineOptions.JARS,
                            jars.stream().distinct().collect(Collectors.toList()));
                    List<String> classpath = configuration.get(PipelineOptions.CLASSPATHS);
                    if (classpath == null) {
                        classpath = new ArrayList<>();
                    }
                    classpath.addAll(
                            pluginPaths.stream().map(URL::toString).collect(Collectors.toList()));
                    configuration.set(
                            PipelineOptions.CLASSPATHS,
                            classpath.stream().distinct().collect(Collectors.toList()));
                });
    }

    public StreamExecutionEnvironment getStreamExecutionEnvironment() {
        return environment;
    }

    public StreamTableEnvironment getStreamTableEnvironment() {
        return tableEnvironment;
    }

    private void createStreamTableEnvironment() {
        EnvironmentSettings environmentSettings =
                EnvironmentSettings.newInstance().inStreamingMode().build();
        tableEnvironment =
                StreamTableEnvironment.create(getStreamExecutionEnvironment(), environmentSettings);
        TableConfig config = tableEnvironment.getConfig();
        if (EnvironmentUtil.hasPathAndWaring(this.config, ConfigKeyName.MAX_STATE_RETENTION_TIME)
                && EnvironmentUtil.hasPathAndWaring(
                        this.config, ConfigKeyName.MIN_STATE_RETENTION_TIME)) {
            long max = this.config.getLong(ConfigKeyName.MAX_STATE_RETENTION_TIME);
            long min = this.config.getLong(ConfigKeyName.MIN_STATE_RETENTION_TIME);
            config.setIdleStateRetentionTime(Time.seconds(min), Time.seconds(max));
        }
        // init flink table env config
        EnvironmentUtil.initTableEnvironmentConfiguration(this.config, config.getConfiguration());
    }

    private void createStreamEnvironment() {
        Configuration configuration = new Configuration();
        if(StringUtils.isNotBlank(flinkCommandArgs.getJobId())) {
            configuration.setString(PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID, flinkCommandArgs.getJobId());
        }
        EnvironmentUtil.initConfiguration(config, configuration);
        environment = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        setTimeCharacteristic();

        setCheckpoint();

        EnvironmentUtil.setRestartStrategy(config, environment.getConfig());

        if (EnvironmentUtil.hasPathAndWaring(config, ConfigKeyName.BUFFER_TIMEOUT_MILLIS)) {
            long timeout = config.getLong(ConfigKeyName.BUFFER_TIMEOUT_MILLIS);
            environment.setBufferTimeout(timeout);
        }

        if (config.hasPath(EnvCommonOptions.PARALLELISM.key())) {
            int parallelism = config.getInt(EnvCommonOptions.PARALLELISM.key());
            environment.setParallelism(parallelism);
        } else if (config.hasPath(ConfigKeyName.PARALLELISM)) {
            log.warn(
                    "the parameter 'execution.parallelism' will be deprecated, please use common parameter 'parallelism' to set it");
            int parallelism = config.getInt(ConfigKeyName.PARALLELISM);
            environment.setParallelism(parallelism);
        }

        if (EnvironmentUtil.hasPathAndWaring(config, ConfigKeyName.MAX_PARALLELISM)) {
            int max = config.getInt(ConfigKeyName.MAX_PARALLELISM);
            environment.setMaxParallelism(max);
        }

        if (this.jobMode.equals(JobMode.BATCH)) {
            environment.setRuntimeMode(RuntimeExecutionMode.BATCH);
        }
    }

    private void setTimeCharacteristic() {
        if (EnvironmentUtil.hasPathAndWaring(config, ConfigKeyName.TIME_CHARACTERISTIC)) {
            String timeType = config.getString(ConfigKeyName.TIME_CHARACTERISTIC);
            switch (timeType.toLowerCase()) {
                case "event-time":
                    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
                    break;
                case "ingestion-time":
                    environment.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
                    break;
                case "processing-time":
                    environment.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
                    break;
                default:
                    log.warn(
                            "set time-characteristic failed, unknown time-characteristic [{}],only support event-time,ingestion-time,processing-time",
                            timeType);
                    break;
            }
        }
    }

    private void setCheckpoint() {
        if (jobMode == JobMode.BATCH) {
            log.warn(
                    "Disabled Checkpointing. In flink execution environment, checkpointing is not supported and not needed when executing jobs in BATCH mode");
        }
        long interval = 0;
        if (config.hasPath(EnvCommonOptions.CHECKPOINT_INTERVAL.key())) {
            interval = config.getLong(EnvCommonOptions.CHECKPOINT_INTERVAL.key());
        } else if (config.hasPath(ConfigKeyName.CHECKPOINT_INTERVAL)) {
            log.warn(
                    "the parameter 'execution.checkpoint.interval' will be deprecated, please use common parameter 'checkpoint.interval' to set it");
            interval = config.getLong(ConfigKeyName.CHECKPOINT_INTERVAL);
        }

        if (interval > 0) {
            CheckpointConfig checkpointConfig = environment.getCheckpointConfig();
            environment.enableCheckpointing(interval);

            if (EnvironmentUtil.hasPathAndWaring(config, ConfigKeyName.CHECKPOINT_MODE)) {
                String mode = config.getString(ConfigKeyName.CHECKPOINT_MODE);
                switch (mode.toLowerCase()) {
                    case "exactly-once":
                        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
                        break;
                    case "at-least-once":
                        checkpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);
                        break;
                    default:
                        log.warn(
                                "set checkpoint.mode failed, unknown checkpoint.mode [{}],only support exactly-once,at-least-once",
                                mode);
                        break;
                }
            }

            if (EnvironmentUtil.hasPathAndWaring(
                    config, EnvCommonOptions.CHECKPOINT_TIMEOUT.key())) {
                long timeout = config.getLong(EnvCommonOptions.CHECKPOINT_TIMEOUT.key());
                checkpointConfig.setCheckpointTimeout(timeout);
            } else if (EnvironmentUtil.hasPathAndWaring(config, ConfigKeyName.CHECKPOINT_TIMEOUT)) {
                long timeout = config.getLong(ConfigKeyName.CHECKPOINT_TIMEOUT);
                checkpointConfig.setCheckpointTimeout(timeout);
            }

            if (EnvironmentUtil.hasPathAndWaring(config, ConfigKeyName.CHECKPOINT_DATA_URI)) {
                String uri = config.getString(ConfigKeyName.CHECKPOINT_DATA_URI);
                StateBackend fsStateBackend = new FsStateBackend(uri);
                if (EnvironmentUtil.hasPathAndWaring(config, ConfigKeyName.STATE_BACKEND)) {
                    String stateBackend = config.getString(ConfigKeyName.STATE_BACKEND);
                    if ("rocksdb".equalsIgnoreCase(stateBackend)) {
                        StateBackend rocksDBStateBackend =
                                new RocksDBStateBackend(fsStateBackend, TernaryBoolean.TRUE);
                        environment.setStateBackend(rocksDBStateBackend);
                    }
                } else {
                    environment.setStateBackend(fsStateBackend);
                }
            }

            if (EnvironmentUtil.hasPathAndWaring(
                    config, ConfigKeyName.MAX_CONCURRENT_CHECKPOINTS)) {
                int max = config.getInt(ConfigKeyName.MAX_CONCURRENT_CHECKPOINTS);
                checkpointConfig.setMaxConcurrentCheckpoints(max);
            }

            if (EnvironmentUtil.hasPathAndWaring(config, ConfigKeyName.CHECKPOINT_CLEANUP_MODE)) {
                boolean cleanup = config.getBoolean(ConfigKeyName.CHECKPOINT_CLEANUP_MODE);
                if (cleanup) {
                    checkpointConfig.enableExternalizedCheckpoints(
                            CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);
                } else {
                    checkpointConfig.enableExternalizedCheckpoints(
                            CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
                }
            }

            if (EnvironmentUtil.hasPathAndWaring(
                    config, ConfigKeyName.MIN_PAUSE_BETWEEN_CHECKPOINTS)) {
                long minPause = config.getLong(ConfigKeyName.MIN_PAUSE_BETWEEN_CHECKPOINTS);
                checkpointConfig.setMinPauseBetweenCheckpoints(minPause);
            }

            if (EnvironmentUtil.hasPathAndWaring(
                    config, ConfigKeyName.FAIL_ON_CHECKPOINTING_ERRORS)) {
                int failNum = config.getInt(ConfigKeyName.FAIL_ON_CHECKPOINTING_ERRORS);
                checkpointConfig.setTolerableCheckpointFailureNumber(failNum);
            }
        }
    }

    public void registerResultTable(Config config, DataStream<Row> dataStream, String name) {
        StreamTableEnvironment tableEnvironment = this.getStreamTableEnvironment();
        if (!TableUtil.tableExists(tableEnvironment, name)) {
            tableEnvironment.createTemporaryView(
                    name, tableEnvironment.fromChangelogStream(dataStream));
        }
    }

    public static FlinkRuntimeEnvironment getInstance(Config config,FlinkCommandArgs flinkCommandArgs) {
        if (INSTANCE == null) {
            synchronized (FlinkRuntimeEnvironment.class) {
                if (INSTANCE == null) {
                    INSTANCE = new FlinkRuntimeEnvironment(config,flinkCommandArgs);
                }
            }
        }
        return INSTANCE;
    }
}
