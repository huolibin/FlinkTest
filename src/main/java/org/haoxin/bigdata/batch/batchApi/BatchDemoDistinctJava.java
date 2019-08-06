package org.haoxin.bigdata.batch.batchApi;

import org.apache.flink.api.common.functions.FlatMapFunction;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.util.Collector;


import java.util.ArrayList;


/**
 * distinct 去重
 *
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2019/7/31 16:23
 */
public class BatchDemoDistinctJava {
    public static void main(String[] args)  throws Exception{

        //获取环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        ArrayList<String> data = new ArrayList<>();
        data.add("hello you");
        data.add("hello me");

        DataSource<String> text = env.fromCollection(data);

        FlatMapOperator<String, String> flatMapData = text.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] split = value.split("\\W+");
                for (String s : split) {
                    System.out.println("单词是：" + s);
                    out.collect(s);
                }
            }
        });


        //去重打印
        flatMapData.distinct().print();

    }
}
