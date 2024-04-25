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

package org.apache.seatunnel.example.flink.v2;

import org.apache.seatunnel.core.starter.SeaTunnel;
import org.apache.seatunnel.core.starter.exception.CommandException;
import org.apache.seatunnel.core.starter.flink.args.FlinkCommandArgs;

import java.io.FileNotFoundException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;

public class SeaTunnelApiExample {

    public static void main(String[] args)
            throws FileNotFoundException, URISyntaxException, CommandException {
        // k8s args --target kubernetes-application --config s3a:/dolphinscheduler/fake_to_console.conf --name SeaTunnel --access-key 6NTWJGZLSK1Y9F38WC48 --secret-key IY7mBaNTI3Y7u6ZO0x02FqYu87IDOdkELzTlAdM5 --bucket-name dolphinscheduler --end-point http://10.83.4.204:8060
        String configurePath = args.length > 0 ? args[0] : "/examples/fake_to_console.conf";

        String configFile;
        if (configurePath.startsWith("s3a")) {
            configFile = configurePath;
        } else {
            configFile = getTestConfigFile(configurePath);
        }
        FlinkCommandArgs flinkCommandArgs = new FlinkCommandArgs();
        flinkCommandArgs.setConfigFile(configFile);
        flinkCommandArgs.setCheckConfig(false);
        flinkCommandArgs.setVariables(null);
        if (args.length > 1){
            String ak = args[1];
            String sk = args[2];
            String bucketName = args[3];
            String endPoint = args[4];
            String jobId = args[5];
            String shouldSync = args[6];


//            flinkCommandArgs.setAccessKey(ak);
//            flinkCommandArgs.setSecretKey(sk);
//            flinkCommandArgs.setBucketName(bucketName);
//            flinkCommandArgs.setEndPoint(endPoint);
//            flinkCommandArgs.setJobId(jobId);
//            flinkCommandArgs.setShouldSync(shouldSync);
//            flinkCommandArgs.set
        }

//        flinkCommandArgs.
        SeaTunnel.run(flinkCommandArgs.buildCommand());
    }

    public static String getTestConfigFile(String configFile)
            throws FileNotFoundException, URISyntaxException {
        URL resource = SeaTunnelApiExample.class.getResource(configFile);
        if (resource == null) {
            throw new FileNotFoundException("Can't find config file: " + configFile);
        }
        return Paths.get(resource.toURI()).toString();
    }
}
