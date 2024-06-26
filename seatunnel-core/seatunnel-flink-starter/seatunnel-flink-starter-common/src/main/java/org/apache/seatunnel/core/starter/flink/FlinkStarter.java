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

package org.apache.seatunnel.core.starter.flink;

import org.apache.commons.lang3.StringUtils;
import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.core.starter.Starter;
import org.apache.seatunnel.core.starter.enums.EngineType;
import org.apache.seatunnel.core.starter.enums.MasterType;
import org.apache.seatunnel.core.starter.flink.args.FlinkCommandArgs;
import org.apache.seatunnel.core.starter.utils.CommandLineUtils;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** The SeaTunnel flink starter, used to generate the final flink job execute command. */
public class FlinkStarter implements Starter {
    private static final String APP_NAME = SeaTunnelFlink.class.getName();
    public static final String APP_JAR_NAME = EngineType.FLINK15.getStarterJarName();
    public static final String POD_TEMPLATE_FILE_JOBMANAGER ="jobmanager-pod-template.yaml";
    public static final String POD_TEMPLATE_FILE_TASKMANAGER ="taskmanager-pod-template.yaml";
    public static final String SHELL_NAME = EngineType.FLINK15.getStarterShellName();
    private final FlinkCommandArgs flinkCommandArgs;
    private final String appJar;

    FlinkStarter(String[] args) {
        this.flinkCommandArgs =
                CommandLineUtils.parse(args, new FlinkCommandArgs(), SHELL_NAME, true);
        // set the deployment mode, used to get the job jar path.
        Common.setDeployMode(flinkCommandArgs.getDeployMode());
        Common.setStarter(true);
        // local:///opt/seatunnel/starter/seatunnel-flink-15-starter.jar
        if(flinkCommandArgs.getMasterType() == MasterType.KUBERNETES_APPLICATION){
            this.appJar = "local://"+Common.appStarterDir().resolve(APP_JAR_NAME).toString();
        }else{
            this.appJar = Common.appStarterDir().resolve(APP_JAR_NAME).toString();
        }

    }

    public static void main(String[] args) {
        FlinkStarter flinkStarter = new FlinkStarter(args);
        System.out.println(String.join(" ", flinkStarter.buildCommands()));
    }

    @Override
    public List<String> buildCommands() {
        List<String> command = new ArrayList<>();
        // set start command
        command.add("${FLINK_HOME}/bin/flink");
        // set deploy mode, run or run-application
        command.add(flinkCommandArgs.getDeployMode().getDeployMode());
        // set submitted target master
        if (flinkCommandArgs.getMasterType() != null) {
            command.add("--target");
            command.add(flinkCommandArgs.getMasterType().getMaster());
        }

        // set flink k8s  properties
        flinkCommandArgs.getK8sParameters().stream()
                .filter(Objects::nonNull)
                .map(String::trim)
                .forEach(variable -> command.add("-D" + variable));

        // set flink k8s pod-template-file
        if (flinkCommandArgs.getMasterType() == MasterType.KUBERNETES_APPLICATION){
            String jobManagerPodTempaltePath =
                    Common.appConfigDir()
                            .resolve(FlinkStarter.POD_TEMPLATE_FILE_JOBMANAGER)
                            .toString();
            String taskManagerPodTempaltePath = Common.appConfigDir()
                    .resolve(FlinkStarter.POD_TEMPLATE_FILE_TASKMANAGER)
                    .toString();
            if (StringUtils.isNotBlank(jobManagerPodTempaltePath) && StringUtils.isNotBlank(taskManagerPodTempaltePath)) {
                System.out.println("Pod template file path: " + jobManagerPodTempaltePath + "," + taskManagerPodTempaltePath);
                command.add("-Dkubernetes.pod-template-file.jobmanager=" + jobManagerPodTempaltePath);
                command.add("-Dkubernetes.pod-template-file.taskmanager=" + taskManagerPodTempaltePath);
            }
        }

        // set flink original parameters
        command.addAll(flinkCommandArgs.getOriginalParameters());
        // set main class name
        command.add("-c");
        command.add(APP_NAME);
        // set main jar name
        command.add(appJar);
        // set config file path
        command.add("--config");
        command.add(flinkCommandArgs.getConfigFile());
        // set check config flag
        if (flinkCommandArgs.isCheckConfig()) {
            command.add("--check");
        }
        // set job name
        command.add("--name");
        command.add(flinkCommandArgs.getJobName());
        // set encryption
        if (flinkCommandArgs.isEncrypt()) {
            command.add("--encrypt");
        }
        // set decryption
        if (flinkCommandArgs.isDecrypt()) {
            command.add("--decrypt");
        }
        // set job id
        if (StringUtils.isNotBlank(flinkCommandArgs.getJobId())){
            command.add("--job-id");
            command.add(flinkCommandArgs.getJobId());
        }
        // set s3 access key
        if (StringUtils.isNotBlank(flinkCommandArgs.getAccessKey())){
            command.add("--access-key");
            command.add(flinkCommandArgs.getAccessKey());
        }
        // set s3 secret key
        if (StringUtils.isNotBlank(flinkCommandArgs.getSecretKey())){
            command.add("--secret-key");
            command.add(flinkCommandArgs.getSecretKey());
        }

        // set s3 bucket name
        if(StringUtils.isNotBlank(flinkCommandArgs.getBucketName())){
            command.add("--bucket-name");
            command.add(flinkCommandArgs.getBucketName());
        }
        // set s3 end point
        if(StringUtils.isNotBlank(flinkCommandArgs.getEndPoint())){
            command.add("--end-point");
            command.add(flinkCommandArgs.getEndPoint());
        }

        // set should sync task state
        if(StringUtils.isNotBlank(flinkCommandArgs.getShouldSync())){
            command.add("--should-sync");
            command.add(String.valueOf(flinkCommandArgs.getShouldSync()));
        }


        // set extra system properties
        flinkCommandArgs.getVariables().stream()
                .filter(Objects::nonNull)
                .map(String::trim)
                .forEach(variable -> command.add("-i " + variable));
        return command;
    }

//    public static void main(String[] args) {
//        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
//        // 假设 pod-template.yaml 位于类路径根目录下，或在名为 "config" 的子目录下
//        URL resourceUrl = classLoader.getResource("pod-template.yaml");
//        // 或者，如果在子目录： classLoader.getResource("config/pod-template.yaml");
//        if (resourceUrl != null) {
//            String filePath = resourceUrl.getFile();
//            System.out.println("Pod template file path: " + filePath);
//        }
//    }
}
