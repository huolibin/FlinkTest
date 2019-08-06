package org.haoxin.bigdata.batch.batchApi;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.CrossOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


/**
 * broadcast广播变量
 *
 * 需求：
 * flink会从数据源中获取到用户的姓名
 *
 * 最终需要把用户的姓名和年龄信息打印出来
 *
 * 分析：
 * 所以就需要在中间的map处理的时候获取用户的年龄信息
 *
 * 建议吧用户的关系数据集使用广播变量进行处理
 *
 *
 * 注意：如果多个算子需要使用同一份数据集，那么需要在对应的多个算子后面分别注册广播变量
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2019/7/31 16:23
 */
public class BatchDemoBroadcastJava {
    public static void main(String[] args)  throws Exception{

        //获取环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //准备广播数据
        ArrayList<Tuple2<String,Integer>> broadData = new ArrayList<>();
        broadData.add(new Tuple2<>("ls",12));
        broadData.add(new Tuple2<>("zs",28));
        broadData.add(new Tuple2<>("ww",53));
        DataSource<Tuple2<String, Integer>> tuple2DataSource = env.fromCollection(broadData);

        //对广播数据处理，把数据集转化成map形式
        MapOperator<Tuple2<String, Integer>, HashMap<String, Integer>> tobroadcast = tuple2DataSource.map(new MapFunction<Tuple2<String, Integer>, HashMap<String, Integer>>() {
            @Override
            public HashMap<String, Integer> map(Tuple2<String, Integer> value) {
                HashMap<String, Integer> StringHashMap = new HashMap<>();
                StringHashMap.put(value.f0, value.f1);
                return StringHashMap;
            }
        });


        //获取源数据
        DataSource<String> data = env.fromElements("ls", "zs", "ww", "aa");

        MapOperator<String, String> result = data.map(new RichMapFunction<String, String>() {
            HashMap<String, Integer> allMap = new HashMap<String, Integer>();

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                List<HashMap<String, Integer>> broadcastData = getRuntimeContext().getBroadcastVariable("tobroadcastName");
                for (HashMap map : broadcastData) {
                    allMap.putAll(map);
                }
            }

            @Override
            public String map(String value) throws Exception {
                Integer age = allMap.get(value);
                return value + "," + age;
            }
        }).withBroadcastSet(tobroadcast, "tobroadcastName");


        result.print();
    }
}
