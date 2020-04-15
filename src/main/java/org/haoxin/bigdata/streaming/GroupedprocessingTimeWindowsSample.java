package org.haoxin.bigdata.streaming;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Random;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2020/4/15 10:36
 *
 * flink 中fold函数执行的demo
 * volatile 关键字的作用是：保证了变量的可见性。被其修饰的变量，如果值发生变化，其他线程立马可见，避免出现脏读的现象。
 *
 */
public class GroupedprocessingTimeWindowsSample {
    private  static final Logger logger = LoggerFactory.getLogger(GroupedprocessingTimeWindowsSample.class);
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        DataStream<Tuple2<String, Integer>> ds = env.addSource(new MyDataSource());
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = ds.keyBy(0);

        //这里只为将datastream-> keyedStream ,用空字符串做分区键，所有数据为相同分区
        keyedStream.sum(1).keyBy(new KeySelector<Tuple2<String, Integer>, Object>() {
            @Override
            public Object getKey(Tuple2<String, Integer> value) throws Exception {
                return "";
            }
        }).fold(new HashMap<String, Integer>(), new FoldFunction<Tuple2<String, Integer>, HashMap<String, Integer>>() {
            @Override
            //这里用HashMap做暂存器
            public HashMap<String, Integer> fold(HashMap<String, Integer> accumulator, Tuple2<String, Integer> value) throws Exception {
                accumulator.put(value.f0,value.f1);
                return accumulator;
            }
        }).addSink(new SinkFunction<HashMap<String, Integer>>() {
            @Override
            public void invoke(HashMap<String, Integer> value, Context context) throws Exception {
                //每个商品的成交量
                System.out.println(value);
                //商品的总成交量
                System.out.println(value.values().stream().mapToInt(v -> v).sum());
            }
        });
        env.execute();
    }

    private static class MyDataSource extends RichParallelSourceFunction<Tuple2<String, Integer>> {


        private volatile boolean isRunning = true;

        @Override
        public void run(SourceContext<Tuple2<String, Integer>> ctx) throws Exception {
            Random random = new Random();
            while (isRunning) {
                Thread.sleep((getRuntimeContext().getIndexOfThisSubtask() + 1) * 1000 * 5);
                String key = "类别" + (char) ('A' + random.nextInt(4));
                int value = random.nextInt(10) + 1;
                System.out.println("input: " + key + ":" + value);
                ctx.collect(new Tuple2<>(key, value));
            }

        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }
}
