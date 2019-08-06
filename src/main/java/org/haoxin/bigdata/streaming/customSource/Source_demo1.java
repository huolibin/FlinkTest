package org.haoxin.bigdata.streaming.customSource;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2019/7/16 17:51
 * 测试MyCustomSource的正确性
 */
public class Source_demo1 {
    public static void main(String[] args) throws Exception {
        //获取环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //获取数据,并行度只能是1
        DataStreamSource<Long> text = env.addSource(new MyCustomSource()).setParallelism(1);
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
