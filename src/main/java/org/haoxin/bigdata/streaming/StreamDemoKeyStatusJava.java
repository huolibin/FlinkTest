package org.haoxin.bigdata.streaming;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 *
 * keys-state
 *
 * 需求是：
 *  对同一个key，每输入两个数据就输出其平均值
 *
 *
 * 注意：transient关键字是用来表示变量将不被序列化处理
 *
 *
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2019/8/7 15:51
 */
public class StreamDemoKeyStatusJava {
    public static void main(String[] args) throws Exception{
        //获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //源数据
        DataStream<Tuple2<Long, Long>> data = env.fromElements(Tuple2.of(1L, 3L), Tuple2.of(1L, 5L), Tuple2.of(1L, 7L), Tuple2.of(1L, 4L), Tuple2.of(1L, 2L));

        data.keyBy(0).flatMap(new CountWindowAverage()).print();
        env.execute("StreamDemoKeyStatusJava");
    }
    public static class CountWindowAverage extends RichFlatMapFunction<Tuple2<Long,Long>,Tuple2<Long,Long>>{
        /**
         * ValueState状态句柄，第一个值为count，第二个值为sum
         */
        private transient ValueState<Tuple2<Long, Long>> sum;
        @Override
        public void flatMap(Tuple2<Long, Long> input, Collector<Tuple2<Long, Long>> out) throws Exception {
            //获取当前的状态值
            Tuple2<Long, Long> currentSum = sum.value();

            //更新
            currentSum.f0 += 1L;
            currentSum.f1 += input.f1;

            //更新状态值
            sum.update(currentSum);

            //如果count>=2，清空状态值，重新计算
            if (currentSum.f0 >= 2){
                out.collect(new Tuple2<>(input.f0, currentSum.f1/currentSum.f0));
                sum.clear();
            }

        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            ValueStateDescriptor<Tuple2<Long, Long>> descriptor = new ValueStateDescriptor<>("average", TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {
            }), Tuple2.of(0L, 0L));

            sum = getRuntimeContext().getState(descriptor);
        }
    }

}
