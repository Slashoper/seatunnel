package org.apache.seatunnel.core.starter.flink.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

public class LinuxShellExecutor {

    /**
     * 执行一个Linux shell命令，并返回其标准输出。
     *
     * @param command 待执行的shell命令，可以是单个命令或带参数的命令列表
     * @return 命令的标准输出字符串
     * @throws InterruptedException 如果线程在等待过程中被中断
     * @throws IOException          如果在读取输出或错误流时发生IO错误
     */
    public static int  executeCommand(List<String> command)  {
        ProcessBuilder processBuilder = new ProcessBuilder(command);
        Process process = null;
        int exitCode = 0;
        try {
            process = processBuilder.start();
            // 读取命令的标准输出
            BufferedReader outputReader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            StringBuilder output = new StringBuilder();
            String line;
            while ((line = outputReader.readLine()) != null) {
                output.append(line).append(System.lineSeparator());
            }

            // 等待命令执行完成
             exitCode = process.waitFor();
        } catch (Exception e) {
           e.printStackTrace();
        }

        return exitCode;


//        if (exitCode != 0) {
//            // 读取命令的标准错误输出（可选）
//            BufferedReader errorReader = new BufferedReader(new InputStreamReader(process.getErrorStream()));
//            StringBuilder error = new StringBuilder();
//            while ((line = errorReader.readLine()) != null) {
//                error.append(line).append(System.lineSeparator());
//            }
//            throw new IOException("Command execution failed with exit code " + exitCode + ". Error output: " + error);
//        }
//
//        return output.toString().trim();
    }

    /**
     * 执行一个Linux shell命令，并向其标准输入写入数据。
     *
     * @param command 待执行的shell命令，可以是单个命令或带参数的命令列表
     * @param input   要写入命令标准输入的数据
     * @return 命令的标准输出字符串
     * @throws InterruptedException 如果线程在等待过程中被中断
     * @throws IOException          如果在读取输出或错误流时发生IO错误，或者写入输入流时发生错误
     */
    public static int executeCommandWithInput(List<String> command, String input) throws InterruptedException, IOException {
        ProcessBuilder processBuilder = new ProcessBuilder(command);
        Process process = processBuilder.start();

        // 写入命令的标准输入
        OutputStream stdin = process.getOutputStream();
        stdin.write(input.getBytes());
        stdin.flush();
        stdin.close();

        // 读取命令的标准输出
        BufferedReader outputReader = new BufferedReader(new InputStreamReader(process.getInputStream()));
        StringBuilder output = new StringBuilder();
        String line;
        while ((line = outputReader.readLine()) != null) {
            output.append(line).append(System.lineSeparator());
        }

        // 等待命令执行完成
        int exitCode = process.waitFor();
        return exitCode;
//        if (exitCode != 0) {
//            // 读取命令的标准错误输出（可选）
//            BufferedReader errorReader = new BufferedReader(new InputStreamReader(process.getErrorStream()));
//            StringBuilder error = new StringBuilder();
//            while ((line = errorReader.readLine()) != null) {
//                error.append(line).append(System.lineSeparator());
//            }
//            throw new IOException("Command execution failed with exit code " + exitCode + ". Error output: " + error);
//        }
//
//        return output.toString().trim();
    }

    // 示例：使用方法
    public static void main(String[] args) throws InterruptedException, IOException {
        List<String> lsCommand = new ArrayList<>();
        lsCommand.add("ls");
        lsCommand.add("-l");

        int lsOutput = executeCommand(lsCommand);
        System.out.println("ls -l output:");
        System.out.println(lsOutput);

        // 示例：向命令提供输入
        List<String> grepCommand = new ArrayList<>();
        grepCommand.add("grep");
        grepCommand.add("example");

        int grepOutput = executeCommandWithInput(grepCommand, "This is an example text.");
        System.out.println("grep example output:");
        System.out.println(grepOutput);
    }
}
