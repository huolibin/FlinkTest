package org.haoxin.bigdata.streaming.operator;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.haoxin.bigdata.streaming.customSource.MyCustomSource;
import org.haoxin.bigdata.streaming.customSource.Source_demo1;

import java.util.ArrayList;

/**
 * 
 * split 可以把一个流数据分成多个流
 *
 * 在工作中，源数据中混合了多种类似的数据，多种类型的数据处理规则不一样，所以可以在根据一定的规则，
 * 把一个数据流切分成多个数据流，这样就可以使用不同的处理逻辑
 * 
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2019/7/18 11:35
 */
public class demo_split {
    public static void main(String[] args) throws Exception {
        //获取环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //获取数据
        DataStreamSource<Long> text = env.addSource(new MyCustomSource()).setParallelism(1);

        //对流进行  切分，按照数据的奇偶进行切分
        SplitStream<Long> split = text.split(new OutputSelector<Long>() {
            @Override
            public Iterable<String> select(Long value) {
                ArrayList<String> outPut = new ArrayList<>();
                if (value % 2 == 0) {
                    outPut.add("even");
                } else {
                    outPut.add("edd");
                }
                return outPut;
            }
        });
        DataStream<Long> even = split.select("even");
        //打印结果
        even.print().setParallelism(1);

        String jobName = Source_demo1.class.getSimpleName();
        env.execute(jobName);
    }
}
