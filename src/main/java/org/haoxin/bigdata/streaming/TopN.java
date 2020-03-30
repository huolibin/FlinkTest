package org.haoxin.bigdata.streaming;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.Comparator;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2020/3/30 15:23
 * <p>
 * <p>
 * 需求：
 * 每个五秒钟 计算过去一小时的top 3 商品
 */
public class TopN {
    public static void main(String[] args) throws Exception {

        //flink运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //确定处理时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        //kafka source
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.71.10:9092");

        FlinkKafkaConsumer010<String> input = new FlinkKafkaConsumer010<>("topn", new SimpleStringSchema(), properties);
        //消费者消费位置，从开始消费
        input.setStartFromEarliest();
        DataStream<String> stream = env.addSource(input);

        //对数据处理
        //将输入语句初始化count值为1的Tuple2<String, Integer>类型
        DataStream<Tuple2<String, Integer>> flatMap = stream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                out.collect(new Tuple2<>(value, 1));
            }
        });

        //key之后的元素进入一个总时间长度为600s,每5s向后滑动一次的滑动窗口
        DataStream<Tuple2<String, Integer>> wcount = flatMap.keyBy(0)
                .window(SlidingProcessingTimeWindows.of(Time.seconds(3600), Time.seconds(5)))
                .sum(1);

        wcount.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5)))//(shu1,12),(shu2,43),(shu4,876)
                .process(new TopNAllFunction(3))
                .print();

        env.execute();

    }

    private static class TopNAllFunction extends ProcessAllWindowFunction<Tuple2<String, Integer>, String, TimeWindow> {

        private int topsize = 3;

        public TopNAllFunction(int topsize) {
            this.topsize = topsize;
        }

        @Override
        public void process(Context context, Iterable<Tuple2<String, Integer>> elements, Collector<String> out) throws Exception {

            //定义一个treemap，treemap按照key降序排列，相同count值不覆盖
            TreeMap<Integer, Tuple2<String, Integer>> treeMap = new TreeMap<>(new Comparator<Integer>() {
                @Override
                public int compare(Integer o1, Integer o2) {
                    return (o1 > o2) ? 1 : -1;
                }
            });

            //对element数据存储到treemap
            for (Tuple2<String, Integer> element : elements) {
                treeMap.put(element.f1, element);
                //只保留前N个元素
                if (treeMap.size() > topsize) {
                    treeMap.pollLastEntry();
                }
            }

            //遍历treemap
            for (Map.Entry<Integer, Tuple2<String, Integer>> entry : treeMap
                    .entrySet()) {
                out.collect("=================\n热销图书列表:\n" + new Timestamp(System.currentTimeMillis()) + treeMap.toString() + "\n===============\n");
            }
        }


    }
}
