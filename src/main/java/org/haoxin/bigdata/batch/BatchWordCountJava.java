package org.haoxin.bigdata.batch;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2019/7/3 18:39
 */
public class BatchWordCountJava {
    public static void main(String[] args) throws Exception {
        //获取环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //读取数据
        DataSource<String> source = env.readTextFile("e:/ceshi/words.txt");
        //整理数据
        AggregateOperator<Tuple2<String, Integer>> wordCounts = source.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] tokens = value.split(" ");
                for (String w : tokens) {
                    if (w.length() > 0) {
                        out.collect(new Tuple2<String, Integer>(w, 1));
                    }
                }
            }
        }).groupBy(0).sum(1);

        //打印
        wordCounts.print();

    }

}
