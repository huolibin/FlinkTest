package org.haoxin.bigdata.batch.batchApi;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


/**
 * broadcast广播变量

 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2019/7/31 16:23
 */
public class BatchDemoTestJava {
    public static void main(String[] args)  throws Exception{

        //获取环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();


        //准备广播数据
        ArrayList<Tuple2<String,Integer>> broadData = new ArrayList<>();
        broadData.add(new Tuple2<>("ls",12));
        broadData.add(new Tuple2<>("zs",28));
        broadData.add(new Tuple2<>("ww",53));
        DataSource<Tuple2<String, Integer>> tuple2DataSource = env.fromCollection(broadData);

        tuple2DataSource.print();

    }
}
