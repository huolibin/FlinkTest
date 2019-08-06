package org.haoxin.bigdata.batch.batchApi;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.configuration.Configuration;


import java.util.ArrayList;


/**
 * 全局累加器
 * counter 计数器
 * 需求：
 *     计算map函数中处理了多少条数据
 *
 * 注意：
 *    只有在任务执行结束后，才能获取到累加器的值
 *
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2019/7/31 16:23
 */
public class BatchDemoCounterJava {
    public static void main(String[] args)  throws Exception{

        //获取环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //获取源数据
        DataSource<String> data = env.fromElements("a", "b", "c", "d", "e");

        MapOperator<String, String> result = data.map(new RichMapFunction<String, String>() {
            //1.先创建累加器
            private IntCounter numline = new IntCounter();

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                //2.注册累加器
                getRuntimeContext().addAccumulator("num-lines", numline);
            }

            @Override
            public String map(String value) throws Exception {
                numline.add(1);
                return value;
            }
        }).setParallelism(4);

        result.writeAsText("e:\\ceshi\\count1");


        JobExecutionResult jobResult = env.execute("counter");
        //3.获取累加器
        int num = jobResult.getAccumulatorResult("num-lines");
        System.out.println("num:"+num);

    }
}
