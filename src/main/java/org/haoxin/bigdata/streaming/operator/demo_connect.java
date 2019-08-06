package org.haoxin.bigdata.streaming.operator;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.haoxin.bigdata.streaming.customSource.MyCustomSource;
import org.haoxin.bigdata.streaming.customSource.Source_demo1;

/**
 * 
 * connect和union类似
 * 但是只能连接两个流，两个流的数据类型可以不同，会对两个流中的数据应用不同的处理方法
 * 
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2019/7/18 11:35
 */
public class demo_connect {
    public static void main(String[] args) throws Exception {
        //获取环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //获取数据
        DataStreamSource<Long> text1 = env.addSource(new MyCustomSource()).setParallelism(1);
        DataStreamSource<Long> first_text2 = env.addSource(new MyCustomSource()).setParallelism(1);
        SingleOutputStreamOperator<String> text2 = first_text2.map(new MapFunction<Long, String>() {
            @Override
            public String map(Long value) throws Exception {
                return "_" + value;
            }
        });

        //合并两个流数据
        ConnectedStreams<Long, String> text = text1.connect(text2);
        
        //对两个流数据处理
        SingleOutputStreamOperator<Object> map = text.map(new CoMapFunction<Long, String, Object>() {
            @Override
            public Long map1(Long aLong) throws Exception {
                return aLong;
            }

            @Override
            public String map2(String s) throws Exception {
                return s;
            }
        });

        //打印结果
        map.print().setParallelism(1);

        String jobName = Source_demo1.class.getSimpleName();
        env.execute(jobName);
    }
}
