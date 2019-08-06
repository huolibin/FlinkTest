package org.haoxin.bigdata.streaming.operator;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.haoxin.bigdata.streaming.customSource.MyCustomSource;
import org.haoxin.bigdata.streaming.customSource.Source_demo1;

/**
 * union 合并多个流，
 * 新的流 会包含所有流中的数据，但是union是一个限制，就是所有合并的流类型必须是一致的
 *
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2019/7/18 11:35
 */
public class demo_union {
    public static void main(String[] args) throws Exception {

        //获取环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //获取数据
        DataStreamSource<Long> text1 = env.addSource(new MyCustomSource()).setParallelism(1);
        DataStreamSource<Long> text2 = env.addSource(new MyCustomSource()).setParallelism(1);

        DataStream<Long> text = text1.union(text2);
        //整理数据
        SingleOutputStreamOperator<Long> map = text.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long value) throws Exception {
                System.out.println("接收到数据：" + value);
                return value;
            }
        });

        //每2秒处理一次数据
        SingleOutputStreamOperator<Long> sum = map.timeWindowAll(Time.seconds(2)).sum(0);

        //打印结果
        sum.print().setParallelism(1);

        String jobName = Source_demo1.class.getSimpleName();
        env.execute(jobName);
    }
}
