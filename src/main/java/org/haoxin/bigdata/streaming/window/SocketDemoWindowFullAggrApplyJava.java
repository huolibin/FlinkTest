package org.haoxin.bigdata.streaming.window;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * window 聚合分类之全量聚合
 * 全量聚合是指所有数据都到了才计算
 * apply 通过windowFunction来实现,跟process相比，少了context。
 *
 * Created by xuwei.tech on 2018/10/8.
 */
public class SocketDemoWindowFullAggrApplyJava {

    public static void main(String[] args) throws Exception{
        //获取需要的端口号
        int port;
        try {
            ParameterTool parameterTool = ParameterTool.fromArgs(args);
            port = parameterTool.getInt("port");
        }catch (Exception e){
            System.err.println("No port set. use default port 9000--java");
            port = 8902;
        }

        //获取flink的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        String hostname = "192.168.71.10";
        String delimiter = "\n";
        //连接socket获取输入的数据
        DataStreamSource<String> text = env.socketTextStream(hostname, port, delimiter);

        //把数据转换成tuple2类型
        DataStream<Tuple2<Integer, Integer>> intData = text.map(new MapFunction<String, Tuple2<Integer, Integer>>() {
            @Override
            public Tuple2<Integer, Integer> map(String value) {
                return new Tuple2<>(1, Integer.parseInt(value));
            }
        });

        intData.keyBy(0)
                .timeWindow(Time.seconds(5))
                .apply(new WindowFunction<Tuple2<Integer, Integer>, String, Tuple, TimeWindow>() {
                    @Override
                    public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple2<Integer, Integer>> input, Collector<String> out) throws Exception {
                        Tuple2<Integer, Integer> integerTuple2 = new Tuple2<>(0,0);
                        long count=0;
                        for (Tuple2<Integer,Integer> e:input) {
                            count++;
                            Integer f0 = integerTuple2.f0;
                            Integer f1 = integerTuple2.f1;
                            Integer e0 = e.f0;
                            Integer e1 = e.f1;
                            integerTuple2.setFields(f0,f1+e1);

                        }
                        out.collect("window:"+window.toString()+", count:"+count+", 聚合结果:"+integerTuple2);
                    }
                }).print();


        //这一行代码一定要实现，否则程序不执行
        env.execute("Socket window full aggr");

    }

}
