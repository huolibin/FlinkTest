package org.haoxin.bigdata.streaming.window;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

/**
 * watermark 的应用
 *
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2019/8/13 11:48
 */
public class SocketDemoWindowWatermarkJava {
    public static void main(String[] args) throws Exception{
        //获取flink运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置并行度
        env.setParallelism(1);

        //设置的eventtime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //socket端口
        int port = 8902;
        String hostname = "192.168.71.10";
        String gefu = "\n";

        //连接socket获取数据
        DataStreamSource<String> text = env.socketTextStream(hostname, port, gefu);

        //解析输入的数据
        DataStream<Tuple2<String, Long>> inputMap = text.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String value) {

                if (value.contains(",")) {
                    String[] arr = value.split(",");
                    return new Tuple2<>(arr[0], Long.parseLong(arr[1]));
                }
                return null;
            }
        });

        //抽取timestamp和生成watermark
        DataStream<Tuple2<String, Long>> watermarksStream = inputMap.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple2<String, Long>>() {
            private final long maxOutOfOrderness = 10000; // 最大乱序时间10秒
            private long currentMaxTimestamp = 0L;  //当前最大时间

            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

            @Override
            public Watermark getCurrentWatermark() {
                return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
            }

            @Override
            public long extractTimestamp(Tuple2<String, Long> element, long previousElementTimestamp) {
                Long timestamp = element.f1;//确定event_time
                currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
                long id = Thread.currentThread().getId();
                System.out.println("currentThreadId:"+id+",key:"+element.f0+",eventtime:["+element.f1+"|"+sdf.format(element.f1)+"],currentMaxTimestamp:["+currentMaxTimestamp+"|"+
                        sdf.format(currentMaxTimestamp)+"],watermark:["+getCurrentWatermark().getTimestamp()+"|"+sdf.format(getCurrentWatermark().getTimestamp())+"]");
                return timestamp;
            }
        });

        //对数据进行窗口计算
        DataStream<String> applyData = watermarksStream.keyBy(0).window(TumblingEventTimeWindows.of(Time.seconds(3)))//按照消息的EventTime分配窗口，和调用TimeWindow效果一样
                .allowedLateness(Time.seconds(2))  //允许数据迟到2秒
                .apply(new WindowFunction<Tuple2<String, Long>, String, Tuple, TimeWindow>() {
                    @Override
                    public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple2<String, Long>> input, Collector<String> out) throws Exception {
                        Iterator<Tuple2<String, Long>> it = input.iterator();
                        ArrayList<Long> longs = new ArrayList<>();
                        while (it.hasNext()) {
                            Tuple2<String, Long> next = it.next();
                            longs.add(next.f1);
                        }
                        Collections.sort(longs);
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                        String result = tuple.toString() + "," + longs.size() + "," + sdf.format(longs.get(0)) + "," + sdf.format(longs.get(longs.size() - 1))
                                + "," + sdf.format(window.getStart()) + "," + sdf.format(window.getEnd());
                        out.collect(result);
                    }
                });

        //打印结果到控制台
        applyData.print();

        //提交
        env.execute("eventtime-watermark");

    }
}
