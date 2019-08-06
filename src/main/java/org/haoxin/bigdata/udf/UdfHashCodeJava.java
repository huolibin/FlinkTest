package org.haoxin.bigdata.udf;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.util.Collector;



/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2019/7/8 15:23
 * hashcode 实现udf继承
 */
public class UdfHashCodeJava {
    public static void main(String[] args) throws Exception {
        int port;
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        port = parameterTool.getInt("port",9000);
        //获取环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

        //注册函数

        //获取数据
        DataSet<String> dataSource = env.readTextFile("e:/ceshi/word.txt");
        dataSource.print();

        MapOperator<String, WordOne> map = dataSource.map(new MapFunction<String, WordOne>() {
            @Override
            public WordOne map(String value) throws Exception {
                String[] split = value.split(" ");
                return new WordOne(split[0], Long.parseLong(split[1]));
            }
        });
        map.print();

        //注册table
        tableEnv.registerDataSet("aaa", map, "word, som");

        //执行sql
        //String sql = "select word, sum(som) as som from aaa group by word";
        String sql = "select * from aaa";
        Table table = tableEnv.sqlQuery(sql);
        DataSet<WordOne> result = tableEnv.toDataSet(table, WordOne.class);
        //打印数据
        result.print();
        env.execute("UdfHashCodeJava");

    }

    public static class WordOne{
        private String word;
        private Long som;

        public WordOne() {
        }

        public WordOne(String word, Long som) {
            this.word = word;
            this.som = som;
        }

        @Override
        public String toString() {
            return word + ":" + som;
        }
    }
}
