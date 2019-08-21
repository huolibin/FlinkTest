package org.haoxin.bigdata.flink.streaming.kafka

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, FlinkKafkaProducer010}

/**
  *
  *
  *
  * @author huolibin@haoxin.cn
  * @date Created by sheting on 2019/8/16 10:07
  *
  */
object StreamingKafkaSinkScala {
  def main(args: Array[String]): Unit = {

    //获取flink运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._


    //checkpoint配置
    env.enableCheckpointing(60000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)
    env.getCheckpointConfig.setCheckpointTimeout(100000)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    //设置statebackend
    //env.setStateBackend(new RocksDBStateBackend("hdfs://hdp1:9000/flink/checkpoints",true));

    //构建socket数据源
    val text = env.socketTextStream("192.168.71.10",8902)



    val topic = "t1"
    val prop = new Properties()
    prop.setProperty("bootstrap.servers","192.168.71.10:9092,192.168.71.11:9092,192.168.71.12:9092")

    //kafka链接
    val myproducer = new FlinkKafkaProducer010[String](topic,new SimpleStringSchema(),prop)


    text.addSink(myproducer)


    env.execute("StreamingKafkaSinkScala")
  }

}
