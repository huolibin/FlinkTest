package org.haoxin.bigdata.batch.batchApi;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.JoinOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.ArrayList;


/**
 * outerjoin 外连接
 *
 * 左连接
 * 右链接
 * 全连接
 *
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2019/7/31 16:23
 */
public class BatchDemoOutJoinJava {
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
        System.out.println("================================源数据是==========================================");
        text1.print();
        text2.print();

        /**
         * 左连接
         */
        System.out.println("================================左连接==========================================");
        text1.leftOuterJoin(text2).where(0)
                .equalTo(0)
                .with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {
                    @Override
                    public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                        if(second == null){
                            return new Tuple3<>(first.f0,first.f1,"null");
                        }else {
                            return new Tuple3<>(first.f0,first.f1,second.f1);
                        }
                    }
                }).print();


        /**
         * 右连接
         */
        System.out.println("================================右连接==========================================");
        text1.rightOuterJoin(text2).where(0)
                .equalTo(0)
                .with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {
                    @Override
                    public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                        if(first == null){
                            return new Tuple3<>(second.f0,"null",second.f1);
                        }else {
                            return new Tuple3<>(first.f0,first.f1,second.f1);
                        }
                    }
                }).print();



        /**
         * 全连接
         */
        System.out.println("================================全连接==========================================");
        text1.fullOuterJoin(text2).where(0)
                .equalTo(0)
                .with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {
                    @Override
                    public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                        if(first == null){
                            return new Tuple3<>(second.f0,"null",second.f1);
                        }else if (second == null){
                            return new Tuple3<>(first.f0,first.f1,"null");
                        }
                        else {
                            return new Tuple3<>(first.f0,first.f1,second.f1);
                        }
                    }
                }).print();

        System.out.println("==================================over =======================================");

    }
}
