package org.haoxin.bigdata.streaming.kafka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchemaWrapper;

import java.util.Properties;

/**
 * kafka11 sink,可以提供exactly-once的语义
 * 但是需要选择具体定义
 * Semantic.NONE
 * Semantic.AT_LEAST_ONCE【默认】
 * Semantic.EXACTLY_ONCE
 *
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2019/8/14 17:37
 */
public class StreamingKafkaSink2 {
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
        //FlinkKafkaProducer011<String> myProducer = new FlinkKafkaProducer011<>(brokerList, topic, new SimpleStringSchema());


        //第一种解决方案，设置FlinkKafkaProducer011里面的事务超时时间
        //设置事务超时时间
        //prop.setProperty("transaction.timeout.ms",60000*15+"");

        //第二种解决方案，设置kafka的最大事务超时时间:在kafka的server-properies 中添加transaction.timeout.ms=60000*15


        //使用仅一次语义的kafkaProducer
        FlinkKafkaProducer011<String> myProducer = new FlinkKafkaProducer011<>(topic, new KeyedSerializationSchemaWrapper<String>(new SimpleStringSchema()), prop, FlinkKafkaProducer011.Semantic.EXACTLY_ONCE);


        //写入kafka
        text.addSink(myProducer);

        env.execute("StreamingKafkaSink");
    }
}
