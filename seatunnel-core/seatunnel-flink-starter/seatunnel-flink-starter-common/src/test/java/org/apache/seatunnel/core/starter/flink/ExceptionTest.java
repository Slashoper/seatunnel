package org.apache.seatunnel.core.starter.flink;

import org.apache.seatunnel.core.starter.exception.TaskExecuteException;

import java.io.PrintWriter;
import java.io.StringWriter;

public class ExceptionTest {

    public static void main(String[] args) {
        try {
            test();
        }catch (Exception e){
//            StringWriter sw = new StringWriter();
//            PrintWriter pw = new PrintWriter(sw);
//            e.printStackTrace(pw);
//            String msg=sw.toString();
//            System.out.println(msg);
            e.printStackTrace();
        }

        System.out.println("hello world");



    }

    public static void test(){
        try {
            int a = 1 / 0;
        }catch (Exception e){
            throw new TaskExecuteException("Execute Flink job error", e);

        }
    }
}
