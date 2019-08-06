package org.haoxin.bigdata.batch.batchApi;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.CrossOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.util.Collector;

import java.util.ArrayList;


/**
 * cross 笛卡尔积运算
 *
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2019/7/31 16:23
 */
public class BatchDemoCrossJava {
    public static void main(String[] args)  throws Exception{

        //获取环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        ArrayList<String> data1 = new ArrayList<>();
        data1.add("hello you");
        data1.add("hello me");

        ArrayList<Integer> data2 = new ArrayList<>();
        data2.add(1);
        data2.add(2);

        DataSource<String> text1 = env.fromCollection(data1);
        DataSource<Integer> text2 = env.fromCollection(data2);

        CrossOperator.DefaultCross<String, Integer> crossData = text1.cross(text2);

        crossData.print();


    }
}
