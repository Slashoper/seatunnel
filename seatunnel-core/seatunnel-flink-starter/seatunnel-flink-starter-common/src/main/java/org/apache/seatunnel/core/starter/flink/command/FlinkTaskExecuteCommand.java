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

package org.apache.seatunnel.core.starter.flink.command;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.seatunnel.core.starter.flink.utils.HttpUtil;
import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigUtil;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigValueFactory;

import org.apache.seatunnel.common.Constants;
import org.apache.seatunnel.common.utils.S3Utils;
import org.apache.seatunnel.core.starter.command.Command;
import org.apache.seatunnel.core.starter.exception.CommandExecuteException;
import org.apache.seatunnel.core.starter.flink.args.FlinkCommandArgs;
import org.apache.seatunnel.core.starter.flink.execution.FlinkExecution;
import org.apache.seatunnel.core.starter.utils.ConfigBuilder;
import org.apache.seatunnel.core.starter.utils.FileUtils;

import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Path;
import java.util.UUID;

import static org.apache.seatunnel.core.starter.utils.FileUtils.checkConfigExist;

@Slf4j
public class FlinkTaskExecuteCommand implements Command<FlinkCommandArgs> {

    private final FlinkCommandArgs flinkCommandArgs;

    public FlinkTaskExecuteCommand(FlinkCommandArgs flinkCommandArgs) {
        this.flinkCommandArgs = flinkCommandArgs;
    }

    @Override
    public void execute() throws CommandExecuteException {
        try {
            Config config;
            if (flinkCommandArgs.getConfigFile().startsWith("s3a")) {
                config = getConf(flinkCommandArgs.getConfigFile());
            } else {
                Path configFile = FileUtils.getConfigPath(flinkCommandArgs);
                checkConfigExist(configFile);
                config = ConfigBuilder.of(configFile);
            }
            // if user specified job name using command line arguments, override config option
            if (!flinkCommandArgs.getJobName().equals(Constants.LOGO)) {
                config =
                        config.withValue(
                                ConfigUtil.joinPath("env", "job.name"),
                                ConfigValueFactory.fromAnyRef(flinkCommandArgs.getJobName()));
            }
            FlinkExecution seaTunnelTaskExecution = new FlinkExecution(config,flinkCommandArgs);
            seaTunnelTaskExecution.execute();
        } catch (Exception e) {
            // before job submitted ,we  also should sync remote task state  before job crash on flink job main entry point
            String shouldSync = flinkCommandArgs.getShouldSync();
            if (shouldSync.equals("1")) {
                Configuration config = GlobalConfiguration.loadConfiguration();
                String ds_taskSate_back_url = config.getString(
                        org.apache.seatunnel.core.starter.flink.constant.Constants.DS_URI, "http://bdp-dolphin-api:12345/dolphinscheduler") + "/task/state/flinkCallBack";
                ObjectMapper mapper = new ObjectMapper();
                ObjectNode objectNode = mapper.createObjectNode();
                objectNode.put("jobId", flinkCommandArgs.getJobId());
                objectNode.put("state", 0);
                StringWriter sw = new StringWriter();
                PrintWriter pw = new PrintWriter(sw);
                e.printStackTrace(pw);
                String exceptionStr = sw.toString();
                objectNode.put("errorMsg", exceptionStr);
                objectNode.put("sourceReceivedBytes",0L);
                objectNode.put("sourceReceivedCount",0L);
                objectNode.put("sinkWritedBytes",0L);
                objectNode.put("sinkWritedCount",0L);
                log.info("===>请求地址:" + ds_taskSate_back_url + ",请求体: " + objectNode.toString());
                HttpUtil.sendPost(ds_taskSate_back_url, objectNode.toString());
                log.info("===>作业异常状态同步完成");
            }
            throw new CommandExecuteException("Flink job executed failed", e);
        }
    }

    private Config getConf(String configFlie) {
        S3Utils s3Utils = S3Utils.getInstance(
                flinkCommandArgs.getAccessKey(), flinkCommandArgs.getSecretKey(), flinkCommandArgs.getBucketName(), flinkCommandArgs.getEndPoint());
        String configFileName = UUID.randomUUID().toString();
        try {
            Path configFile =
                    s3Utils.download(
                            configFlie,
                            Constants.LOCAL_JOBCONFIG_PATH + File.separator + configFileName);
            return ConfigBuilder.of(configFile);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                s3Utils.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
