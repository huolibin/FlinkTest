package org.haoxin.bigdata.sql;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;

import org.apache.flink.table.api.Table;

import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sinks.TableSink;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2020/3/26 15:24
 */
public class Sql2_Test {
    public static void main(String[] args) throws Exception {
        //环境变量
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tableEnv = BatchTableEnvironment.getTableEnvironment(env);
        env.setParallelism(1);

        //数据来源
        DataSource<String> input = env.readTextFile("E:\\intellj_space\\FlinkTest\\test2");
        input.print();

        //数据转成dataset
        DataSet< Order> inputData = input.map(new MapFunction<String, Order>() {
            @Override
            public Order map(String value) throws Exception {
                String[] splits = value.split(" ");
                return new Order(Integer.valueOf(splits[0]), String.valueOf(splits[1]), String.valueOf(splits[2]), Double.valueOf(splits[3]));
            }
        });
        //将Dataset转换为Table
        Table ordertable = tableEnv.fromDataSet(inputData);

        //注册表
        tableEnv.registerTable("Orders",ordertable);

        Table tapiResult = tableEnv.scan("Orders").select("name");
        tapiResult.printSchema();

        Table sqlQuery = tableEnv.sqlQuery("select name,sum(price) as total from Orders group by name");

        //table转换成dataset
        //DataSet<Order> result = tableEnv.toDataSet(sqlQuery, Order.class);

        TableSink sink = new CsvTableSink("E:\\intellj_space\\FlinkTest\\sqltest.txt", "|");
        TableSink sink1 = new CsvTableSink("E:\\intellj_space\\FlinkTest\\sqltest1.txt", "|");

        //存储
        //方式1.writeToSink
        sqlQuery.writeToSink(sink);
        //方式2
        String[] fieldNames = {"name","total"};
        TypeInformation[] fieldTypes = {Types.STRING, Types.DOUBLE};
        tableEnv.registerTableSink("sqltest",fieldNames,fieldTypes,sink1);
        sqlQuery.insertInto("sqltest");

        env.execute();
    }

    public static class Order {
        /**
         * 序号、姓名、书名、价格
         */

        public  Integer id;
        public  String name;
        public  String book;
        public  Double price;

        public Order() {
            super();
        }

        public Order(Integer id, String name, String book, Double price) {
            this.id = id;
            this.name = name;
            this.book = book;
            this.price = price;
        }
    }
    /**
     * 统计结果对应的类
     */
    public static class Result {
        public String name;
        public Double total;

        public Result() {}
    }
}
