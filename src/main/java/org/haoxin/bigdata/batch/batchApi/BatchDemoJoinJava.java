package org.haoxin.bigdata.batch.batchApi;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.JoinOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import java.util.ArrayList;


/**
 * join 内连接
 * 其中的with用法与map相同，可以用map代替
 *
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2019/7/31 16:23
 */
public class BatchDemoJoinJava {
    public static void main(String[] args)  throws Exception{

        //获取环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //id name
        ArrayList<Tuple2<Integer,String>> data1 = new ArrayList<>();
        data1.add(new Tuple2<>(1,"zs"));
        data1.add(new Tuple2<>(2,"ls"));
        data1.add(new Tuple2<>(3,"aa"));
        data1.add(new Tuple2<>(4,"hehe"));

        //id city
        ArrayList<Tuple2<Integer,String>> data2 = new ArrayList<>();
        data2.add(new Tuple2<>(1,"beijing"));
        data2.add(new Tuple2<>(2,"shanghai"));
        data2.add(new Tuple2<>(5,"guangzhou"));

        DataSource<Tuple2<Integer, String>> text1 = env.fromCollection(data1);
        DataSource<Tuple2<Integer, String>> text2 = env.fromCollection(data2);
        JoinOperator.EquiJoin<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>> withData = text1.join(text2).where(0)//指定第一个数据集中需要进行比较的元素角标
                .equalTo(0)//指定第二个数据集中需要进行比较的元素角标
                .with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {
                    @Override
                    public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                        return new Tuple3<>(first.f0, first.f1, second.f1);
                    }
                });


        //打印
        withData.print();

    }
}
