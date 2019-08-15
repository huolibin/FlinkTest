package org.haoxin.bigdata.streaming.kafka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;

import java.util.Properties;

/**
 * kafka10 sink,不会exactly-once的语义，
 * 提供at-least-once的语义，还需要配置下面两个参数
 * setLogFailuresOnly(false)
 * setFlushOnCheckpoint(true)
 * 还要修改kafka生产者的重试次数
 * retries【这个参数的值默认是0】
 *
 *
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2019/8/14 17:37
 */
public class StreamingKafkaSink {
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

        //获取socket数据源数据
        DataStreamSource<String> text = env.socketTextStream("192.168.71.10", 8902, "\n");

        //构造kafka connect
        String topic = "t1";
        String brokerList = "192.168.71.10:9092,192.168.71.11:9092,192.168.71.12:9092";
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers",brokerList);

        FlinkKafkaProducer010<String> myProducer = new FlinkKafkaProducer010<>(brokerList, topic, new SimpleStringSchema());

        //写入kafka
        text.addSink(myProducer);

        env.execute("StreamingKafkaSink");
    }
}
