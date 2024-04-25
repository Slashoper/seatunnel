package org.apache.seatunnel.core.starter.flink.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.JobID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.URL;
import java.net.URLConnection;


public class HttpUtil {

	private static Logger logger = LoggerFactory.getLogger(HttpUtil.class);



	/**
	 * 向指定URL发送POST请求
	 *
	 * @Param url 发送请求得URL
	 * @param params
	 *            请求参数，请求参数应该是json的形式
	 * @return URL 所代表远程资源的响应结果
	 */
	public static String sendPost(String url,  String params) {
		PrintWriter out = null;
		BufferedReader in = null;
		String result = "";
		try {

			URL realUrl = new URL(url);
			// 打开和URL之间得连接
			URLConnection conn = realUrl.openConnection();
			// 设置通用得请求属性
			conn.setRequestProperty("accept", "*/*");
			conn.setRequestProperty("Content-Type", "application/json");
			conn.setRequestProperty("connection", "Keep-Alive");
			conn.setRequestProperty("userAgent", "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1;SV1)");
			// 发送post请求必须设置如下两行
			conn.setDoOutput(true);
			conn.setDoInput(true);
			// 获取URLConnection对象对应的输出流
			out = new PrintWriter(conn.getOutputStream());
			// 发送请求参数
			out.print(params);
			// flush输出流的缓冲
			out.flush();
			in = new BufferedReader(new InputStreamReader(conn.getInputStream()));

			String line;
			while ((line = in.readLine()) != null) {
				result += line;
			}
		} catch (Exception e) {
			logger.error("发生POST请求出现异常！ " + e.getMessage());
			e.printStackTrace();
		} finally {
			try {
				if (out != null) {
					out.close();
				}
				if (in != null) {
					in.close();
				}
			} catch (Exception e2) {
				e2.printStackTrace();
			}
		}
		return result;
	}

	public static void main(String[] args) {
//		String hex = "hello world";
//
//		byte[] bts = new byte[hex.length() / 2];
//
//		for(int i = 0; i < bts.length; ++i) {
//			bts[i] = (byte)Integer.parseInt(hex.substring(2 * i, 2 * i + 2), 16);
//		}
//
//
//		System.out.println(bts);
		ObjectMapper mapper = new ObjectMapper();
		ObjectNode objectNode = mapper.createObjectNode();
		objectNode.put("jobId", "a687b5f08ce921ac95e2257693e40a74");
		objectNode.put("state", 0);
		objectNode.put("sourceReceivedBytes",0L);
		objectNode.put("sourceReceivedCount",0L);
		objectNode.put("sinkWritedBytes",0L);
		objectNode.put("sinkWritedCount",0L);
		objectNode.put("runtime",100L);
		objectNode.put("errorMsg", "org.apache.seatunnel.common.exception.SeaTunnelRuntimeException: ErrorCode:[API-09], ErrorDescription:[Handle save mode failed]");
		System.out.println("===>请求地址:" + "http://localhost:12345/dolphinscheduler/task/state/flinkCallBack" + ",请求体: "+objectNode.toString());
		//            objectNode.put("runtime",jobExecutionResult.getNetRuntime());
		HttpUtil.sendPost("http://localhost:12345/dolphinscheduler/task/state/flinkCallBack", objectNode.toString());
	}

}
