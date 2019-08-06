package org.haoxin.bigdata.sql;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;


import java.util.ArrayList;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2019/7/8 14:33
 */
public class Sql_test_java {
    public static void main(String[] args) throws Exception {
        //获取环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);

        //获取数据，并整理数据
        String words = "hello flink hello imooc";
        String[] split = words.split("\\W+");
        ArrayList<WC> list = new ArrayList();
        for (String word : split) {
            WC wc = new WC(word, 1L);
            list.add(wc);
        }
        DataSet<WC> input = env.fromCollection(list);

        //注册table，并调用sql
        tEnv.registerDataSet("WordCount", input, "word, frequency");

        String sql = "select word,sum(frequency) as frequency from WordCount group by word";
        Table table = tEnv.sqlQuery(sql);
        DataSet<WC> result = tEnv.toDataSet(table, WC.class);

        //打印数据
        result.print();


    }

    public static class WC {
        public String word;
        public long frequency;

        public WC() {
        }

        public WC(String word, long frequency) {
            this.word = word;
            this.frequency = frequency;
        }

        @Override
        public String toString() {
            return word + ":" + frequency;
        }
    }
}