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

package org.apache.seatunnel.common.utils;

import org.apache.seatunnel.common.Constants;

import org.apache.commons.lang3.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.io.FileOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class S3Utils {

    private static final Logger logger = LoggerFactory.getLogger(S3Utils.class);

    private String ACCESS_KEY_ID = "6NTWJGZLSK1Y9F38WC48";

    private String SECRET_KEY_ID = "IY7mBaNTI3Y7u6ZO0x02FqYu87IDOdkELzTlAdM5";

    public static final String REGION = Constants.DEFAULT_S3_REGION;

    private String BUCKET_NAME = "dolphinscheduler";

    private String AWS_END_POINT = "http://10.83.4.204:8060";

    private AmazonS3 s3Client ;

    private static volatile S3Utils s3Utils ;

    private S3Utils(String ak,String sk,String bucketName,String entryPoint) {
        this.ACCESS_KEY_ID = ak;
        this.SECRET_KEY_ID = sk;
        this.BUCKET_NAME = bucketName;
        this.AWS_END_POINT = entryPoint;
        s3Client =
                AmazonS3ClientBuilder.standard()
                        .withPathStyleAccessEnabled(true)
                        .withEndpointConfiguration(
                                new AwsClientBuilder.EndpointConfiguration(
                                        AWS_END_POINT, Regions.fromName(REGION).getName()))
                        .withCredentials(
                                new AWSStaticCredentialsProvider(
                                        new BasicAWSCredentials(ACCESS_KEY_ID, SECRET_KEY_ID)))
                        .build();
        checkBucketNameExists(BUCKET_NAME);
        //        }
    }

//    /** S3Utils single */
//    private enum S3Singleton {
//        INSTANCE;
//
//        private final S3Utils instance;
//
//        S3Singleton() {
//            instance = new S3Utils();
//        }
//
//        private S3Utils getInstance() {
//            return instance;
//        }
//    }


    public static S3Utils getInstance(String ak,String sk,String bucketName,String entryPoint) {
        // 双重检查加锁
        if (s3Utils == null) {
            synchronized (S3Utils.class) {
                // 延迟实例化,需要时才创建
                if (s3Utils == null) {

                    S3Utils temp = null;
                    try {
                        temp = new S3Utils(ak,sk,bucketName,entryPoint);
                    } catch (Exception e) {
                    }
                    if (temp != null)    //为什么要做这个看似无用的操作，因为这一步是为了让虚拟机执行到这一步的时会才对singleton赋值，虚拟机执行到这里的时候，必然已经完成类实例的初始化。所以这种写法的DCL是安全的。由于try的存在，虚拟机无法优化temp是否为null
                        s3Utils = temp;
                }
            }
        }
        return s3Utils;
    }

    public AmazonS3 getS3Client(){
        return this.s3Client;
    }


    public void close() throws IOException {
        s3Client.shutdown();
    }

    public static void main(String[] args) throws Exception {
        String ak = "6NTWJGZLSK1Y9F38WC48";
        String sk = "IY7mBaNTI3Y7u6ZO0x02FqYu87IDOdkELzTlAdM5";
        String bucketName = "dolphinscheduler";
        String entryPoint = "http://10.83.4.204:8060";
        S3Utils s3Utils1 = S3Utils.getInstance(ak,sk,bucketName,entryPoint);
        S3Utils s3Utils2 = S3Utils.getInstance(ak,sk,bucketName,entryPoint);
        System.out.println(s3Utils1.getS3Client() == s3Utils2.getS3Client());
        System.out.println(s3Utils1.exists("1711015209646.log"));
        String configFileName = UUID.randomUUID().toString();

                System.out.println(s3Utils.readFile("/seatunnel/tmp/dolphinscheduler/exec/process/bdp/4/13220792275488_1/12436/363845/seatunnel_SeaTunnelWebTest_363845.conf"));
//        s3Utils.download(
//                "s3a://dolphinscheduler/fake_to_console.conf",
//                Constants.LOCAL_JOBCONFIG_PATH + File.separator + configFileName);
    }

    public boolean exists(String fileName) throws IOException {
        return s3Client.doesObjectExist(BUCKET_NAME, fileName);
    }

    public List<String> readFile(String filePath) throws IOException {
        if (StringUtils.isBlank(filePath)) {
            logger.error("file path:{} is blank", filePath);
            return Collections.emptyList();
        }
        S3Object s3Object = s3Client.getObject(BUCKET_NAME, filePath);
        try (BufferedReader bufferedReader =
                new BufferedReader(new InputStreamReader(s3Object.getObjectContent()))) {
            Stream<String> stream = bufferedReader.lines();
            return stream.collect(Collectors.toList());
        }
    }

    public void checkBucketNameExists(String bucketName) {
        if (StringUtils.isBlank(bucketName)) {
            throw new IllegalArgumentException("resource.aws.s3.bucket.name is blank");
        }

        Bucket existsBucket =
                s3Client.listBuckets().stream()
                        .filter(bucket -> bucket.getName().equals(bucketName))
                        .findFirst()
                        .orElseThrow(
                                () -> {
                                    return new IllegalArgumentException(
                                            "bucketName: "
                                                    + bucketName
                                                    + " is not exists, you need to create them by yourself");
                                });

        logger.info(
                "bucketName: {} has been found, the current regionName is {}",
                existsBucket.getName(),
                REGION);
    }

    public Path download(String srcFilePath, String dstFilePath) throws IOException {
        if (StringUtils.isBlank(srcFilePath)
                || !srcFilePath.startsWith("s3a")
                || srcFilePath.indexOf(BUCKET_NAME) == -1) {
            throw new IllegalArgumentException("the srcFilePath is not a s3 path");
        }
        srcFilePath =
                srcFilePath.substring(
                        srcFilePath.indexOf(BUCKET_NAME) + BUCKET_NAME.length() + 1,
                        srcFilePath.length());
        File dstFile = new File(dstFilePath);
        if (dstFile.isDirectory()) {
            Files.delete(dstFile.toPath());
        } else {
            Files.createDirectories(dstFile.getParentFile().toPath());
        }
        S3Object o = s3Client.getObject(BUCKET_NAME, srcFilePath);
        try (S3ObjectInputStream s3is = o.getObjectContent();
                FileOutputStream fos = new FileOutputStream(dstFilePath)) {
            byte[] readBuf = new byte[1024];
            int readLen;
            while ((readLen = s3is.read(readBuf)) > 0) {
                fos.write(readBuf, 0, readLen);
            }
            logger.info("download {} to the local {} success.", srcFilePath, dstFilePath);
            return dstFile.toPath();
        } catch (AmazonServiceException e) {
            throw new IOException(e.getMessage());
        } catch (FileNotFoundException e) {
            logger.error("the destination file {} not found", dstFilePath);
            throw e;
        }
    }
}
