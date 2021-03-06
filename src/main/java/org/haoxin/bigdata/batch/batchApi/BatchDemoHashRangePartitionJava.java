package org.haoxin.bigdata.batch.batchApi;

import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.CrossOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;


/**
 * Rebalance：对数据集进行再平衡，重分区，消除数据倾斜
 * Hash-Partition：根据指定key的哈希值对数据集进行分区
 * partitionByHash()
 * Range-Partition：根据指定的key对数据集进行范围分区
 * partitionByRange()
 *
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2019/7/31 16:23
 */
public class BatchDemoHashRangePartitionJava {
    public static void main(String[] args)  throws Exception{

        //获取环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(5)));//任务失败 重试


        ArrayList<Tuple2<Integer, String>> data = new ArrayList<>();
        data.add(new Tuple2<>(1,"hello1"));
        data.add(new Tuple2<>(2,"hello2"));
        data.add(new Tuple2<>(2,"hello3"));
        data.add(new Tuple2<>(3,"hello4"));
        data.add(new Tuple2<>(3,"hello5"));
        data.add(new Tuple2<>(3,"hello6"));
        data.add(new Tuple2<>(4,"hello7"));
        data.add(new Tuple2<>(4,"hello8"));
        data.add(new Tuple2<>(4,"hello9"));
        data.add(new Tuple2<>(4,"hello10"));
        data.add(new Tuple2<>(5,"hello11"));
        data.add(new Tuple2<>(5,"hello12"));
        data.add(new Tuple2<>(5,"hello13"));
        data.add(new Tuple2<>(5,"hello14"));
        data.add(new Tuple2<>(5,"hello15"));
        data.add(new Tuple2<>(6,"hello16"));
        data.add(new Tuple2<>(6,"hello17"));
        data.add(new Tuple2<>(6,"hello18"));
        data.add(new Tuple2<>(6,"hello19"));
        data.add(new Tuple2<>(6,"hello20"));
        data.add(new Tuple2<>(6,"hello21"));

        DataSource<Tuple2<Integer, String>> text = env.fromCollection(data);
        //对数据再次平衡，消除数据倾斜
        text.rebalance();

        //partitionByRange 有助于消除数据倾斜
        text.partitionByRange(0).mapPartition(new MapPartitionFunction<Tuple2<Integer, String>, Tuple2<Integer, String>>() {
            @Override
            public void mapPartition(Iterable<Tuple2<Integer, String>> values, Collector<Tuple2<Integer, String>> out) throws Exception {
                Iterator<Tuple2<Integer, String>> it = values.iterator();
                while (it.hasNext()){
                    Tuple2<Integer, String> next = it.next();
                    System.out.println("当前线程id:"+Thread.currentThread().getId()+","+next);
                }
            }
        }).print();

        System.out.println("=====================================================================");


        //partitionByHash 不能友好的消除数据倾斜
        text.partitionByHash(0).mapPartition(new MapPartitionFunction<Tuple2<Integer, String>, Tuple2<Integer, String>>() {
            @Override
            public void mapPartition(Iterable<Tuple2<Integer, String>> values, Collector<Tuple2<Integer, String>> out) throws Exception {
                Iterator<Tuple2<Integer, String>> it = values.iterator();
                while (it.hasNext()) {
                    Tuple2<Integer, String> next = it.next();
                    System.out.println("当前线程id:" + Thread.currentThread().getId() + "," + next);
                }
            }
        }).print();

        System.out.println("=====================================================================");

        //自定义mypartition
        text.partitionCustom(new MyPartition(),0)
    .mapPartition(new MapPartitionFunction<Tuple2<Integer, String>, Tuple2<Integer, String>>() {
            @Override
            public void mapPartition(Iterable<Tuple2<Integer, String>> values, Collector<Tuple2<Integer, String>> out) throws Exception {
                Iterator<Tuple2<Integer, String>> it = values.iterator();
                while (it.hasNext()) {
                    Tuple2<Integer, String> next = it.next();
                    System.out.println("当前线程id:" + Thread.currentThread().getId() + "," + next);
                }
            }
        }).print();

    }
    //自定义mypartition
    public static class MyPartition implements Partitioner<Integer>{
        @Override
        public int partition(Integer key, int numPartitions) {
            System.err.println("分区总数："+ numPartitions);
            return key % 5;
        }
    }
}
