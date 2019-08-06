package org.haoxin.bigdata.streaming.customPartition;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.haoxin.bigdata.streaming.customSource.MyCustomSource;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2019/7/18 16:02
 *
 * 测试 自定义Mypartition
 */
public class DemoMyParition {
    public static void main(String[] args) throws Exception{
        //获取环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //获取数据
        env.setParallelism(2);
        DataStreamSource<Long> text = env.addSource(new MyCustomSource());
        //整理数据
        SingleOutputStreamOperator<Tuple1<Long>> tupleData = text.map(new MapFunction<Long, Tuple1<Long>>() {
            @Override
            public Tuple1<Long> map(Long value) throws Exception {
                return new Tuple1<>(value);
            }
        });
        //分区
        DataStream<Tuple1<Long>> partitionData = tupleData.partitionCustom(new MyPartition(), 0);

        DataStream<Long> result = partitionData.map(new MapFunction<Tuple1<Long>, Long>() {
            @Override
            public Long map(Tuple1<Long> value) {
                System.out.println("当前线程id："+ Thread.currentThread().getId()+",value:"+value);
                return value.getField(0);
            }
        });

        result.print().setParallelism(1);
        env.execute("DemoMyParition");

    }
}
