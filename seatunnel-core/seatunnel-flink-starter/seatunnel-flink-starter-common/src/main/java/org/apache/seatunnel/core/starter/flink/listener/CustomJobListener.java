package org.apache.seatunnel.core.starter.flink.listener;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.JobListener;
import org.apache.seatunnel.common.config.DeployMode;
import org.apache.seatunnel.core.starter.enums.MasterType;
import org.apache.seatunnel.core.starter.flink.args.FlinkCommandArgs;
import org.apache.seatunnel.core.starter.flink.constant.Constants;
import org.apache.seatunnel.core.starter.flink.execution.FlinkExecution;
import org.apache.seatunnel.core.starter.flink.utils.HttpUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.PrintWriter;
import java.io.StringWriter;

public class CustomJobListener implements JobListener {


    private static final Logger LOGGER = LoggerFactory.getLogger(CustomJobListener.class);

    private FlinkCommandArgs flinkCommandArgs;


    public CustomJobListener(FlinkCommandArgs flinkCommandArgs) {
        this.flinkCommandArgs = flinkCommandArgs;
    }

    @Override
    public void onJobSubmitted(@Nullable JobClient jobClient, @Nullable Throwable throwable) {
    }

    @Override
    public void onJobExecuted(@Nullable JobExecutionResult jobExecutionResult, @Nullable Throwable throwable) {
        String  shouldSync = flinkCommandArgs.getShouldSync();
        if(shouldSync.equals("1")) {
            LOGGER.info("===>开始同步状态");
            Configuration config = GlobalConfiguration.loadConfiguration();
            String ds_taskSate_back_url = config.getString(
                    Constants.DS_URI, "http://bdp-dolphin-api:12345/dolphinscheduler") + "/task/state/flinkCallBack";
            syncTaskStateToScheduler(jobExecutionResult, throwable, ds_taskSate_back_url);
        }
    }

    /**
     * 同步作业执行结果给调度平台
     * @param jobExecutionResult
     * @param throwable
     */
    private void syncTaskStateToScheduler(JobExecutionResult jobExecutionResult,Throwable throwable,String ds_taskSate_back_url){

        LOGGER.info("===>sync task state to scheduler " + ds_taskSate_back_url);
        Long sinkWriteBytes = 0L;
        Long sourceReceivedBytes = 0L;
        Long sourceReceivedCount = 0L;
        Long sinkWriteCount = 0L;
        Long runtime = 0L;
        try {
            if (throwable == null) {
                 sinkWriteBytes =  jobExecutionResult.getAccumulatorResult("SinkWriteBytes");
                 sourceReceivedBytes = jobExecutionResult.getAccumulatorResult("SourceReceivedBytes");
                 sourceReceivedCount = jobExecutionResult.getAccumulatorResult("SourceReceivedCount");
                 sinkWriteCount = jobExecutionResult.getAccumulatorResult("SinkWriteCount");
                 runtime = jobExecutionResult.getNetRuntime();

                ObjectMapper mapper = new ObjectMapper();
                ObjectNode objectNode = mapper.createObjectNode();
                objectNode.put("jobId", jobExecutionResult.getJobID().toHexString());
                objectNode.put("state", 1);
                objectNode.put("sourceReceivedBytes",sourceReceivedBytes);
                objectNode.put("sourceReceivedCount",sourceReceivedCount);
                objectNode.put("sinkWritedBytes",sinkWriteBytes);
                objectNode.put("sinkWritedCount",sinkWriteCount);
                objectNode.put("runtime",runtime);
                LOGGER.info("===>请求体: "+objectNode.toString());
                //            objectNode.put("runtime",jobExecutionResult.getNetRuntime());
                HttpUtil.sendPost(ds_taskSate_back_url, objectNode.toString());
                LOGGER.info("===>作业成功状态同步完成");
            } else {
                ObjectMapper mapper = new ObjectMapper();
                ObjectNode objectNode = mapper.createObjectNode();
                objectNode.put("jobId", flinkCommandArgs.getJobId());
                objectNode.put("state", 0);
                objectNode.put("sourceReceivedBytes",sourceReceivedBytes);
                objectNode.put("sourceReceivedCount",sourceReceivedCount);
                objectNode.put("sinkWritedBytes",sinkWriteBytes);
                objectNode.put("sinkWritedCount",sinkWriteCount);
                StringWriter sw = new StringWriter();
                PrintWriter pw = new PrintWriter(sw);
                throwable.printStackTrace(pw);
                String exceptionStr = sw.toString();
                objectNode.put("errorMsg", exceptionStr);
                LOGGER.info("===>请求体: "+objectNode.toString());
                HttpUtil.sendPost(ds_taskSate_back_url, objectNode.toString());
                LOGGER.info("===>作业失败状态同步完成");
            }
        } catch (Exception e) {
            // 消息同步失败
            ObjectMapper mapper = new ObjectMapper();
            ObjectNode objectNode = mapper.createObjectNode();
            objectNode.put("jobId", flinkCommandArgs.getJobId());
            objectNode.put("state", 3);
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            String exceptionStr = sw.toString();
            objectNode.put("errorMsg", exceptionStr);
            objectNode.put("sourceReceivedBytes",sourceReceivedBytes);
            objectNode.put("sourceReceivedCount",sourceReceivedCount);
            objectNode.put("sinkWritedBytes",sinkWriteBytes);
            objectNode.put("sinkWritedCount",sinkWriteCount);
            LOGGER.info("===>请求体: "+objectNode.toString());
            HttpUtil.sendPost(ds_taskSate_back_url, objectNode.toString());
            LOGGER.info("===>作业异常状态同步完成");
            throw  new RuntimeException(e);
        }
    }
}
