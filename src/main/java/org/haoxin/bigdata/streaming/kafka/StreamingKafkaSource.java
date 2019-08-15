package org.haoxin.bigdata.streaming.kafka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.FlinkActor;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;

import java.io.IOException;
import java.util.Properties;

/**
 * kafka source
 *
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2019/8/14 17:37
 */
public class StreamingKafkaSource {
    public static void main(String[] args) throws Exception {
        //获取flink运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //checkpoint配置
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        //设置statebackend
        //env.setStateBackend(new RocksDBStateBackend("hdfs://hdp1:9000/flink/checkpoints",true));

        //构造kafka connect
        String topic = "t1";
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers","192.168.71.10:9092,192.168.71.11:9092,192.168.71.12:9092");
        prop.setProperty("group.id","con1");
        FlinkKafkaConsumer010<String> myConsumer = new FlinkKafkaConsumer010<>(topic, new SimpleStringSchema(), prop);

        myConsumer.setStartFromGroupOffsets();//默认策略

        //获取kafka数据源数据
        DataStreamSource<String> text = env.addSource(myConsumer);

        //控制台打印数据
        text.print().setParallelism(1);

        env.execute("StreamingKafkaSource");
    }
}
