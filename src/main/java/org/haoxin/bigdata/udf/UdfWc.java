package org.haoxin.bigdata.udf;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.util.Collector;
import org.haoxin.bigdata.sql.Sql_test_java;

import java.util.ArrayList;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2019/7/3 17:37
 */
public class UdfWc {
    public static void main(String[] args) throws Exception {
        //获取环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);
        //函数注册于table
        tEnv.registerFunction("StringToSite",new StringToSite(".com"));

        //获取数据，并整理数据
        String words = "hello flink hello imooc";
        String[] split = words.split("\\W+");
        ArrayList<Sql_test_java.WC> list = new ArrayList();
        for (String word : split) {
            Sql_test_java.WC wc = new Sql_test_java.WC(word, 1L);
            list.add(wc);
        }
        DataSet<Sql_test_java.WC> input = env.fromCollection(list);

        //注册table，并调用sql
        tEnv.registerDataSet("WordCount", input, "word, frequency");

        String sql = "select StringToSite(word) as word,sum(frequency) as frequency from WordCount group by word";
        Table table = tEnv.sqlQuery(sql);
        DataSet<Sql_test_java.WC> result = tEnv.toDataSet(table, Sql_test_java.WC.class);

        //打印数据
        result.print();
        env.execute("UdfWc");


    }
    //自定义函数StringToSite hello-> hello.com
    public static class StringToSite extends ScalarFunction{
        private String address;
        public StringToSite(String s) {
            this.address =s;
        }

        public  String eval(String s){
            return s + address;
        }
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
