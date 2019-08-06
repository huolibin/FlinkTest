package org.haoxin.bigdata.flink.kafka

import java.util.Properties

import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * @author huolibin@haoxin.cn
  * @date Created by sheting on 2019/7/15 18:04
  *
  */
object FlinkFromkafakScala {

  private val ZOOKEEPER_HOST = "192.168.xx.xx1:2181,192.168.xx.xx2:2181,192.168.xx.xx3:2181"
  private val KAFKA_BROKER = "192.168.xx.xx1:9092,192.168..xx.xx2:9092,192.168..xx.xx3:9092"
  private val TRANSACTION_GROUP  = "group_id_xx" //定义的消费组
  private val TOPIC = "TOPIC_MQ2KAFKA_DyFusion2" //定义的topic

  def main(args: Array[String]): Unit = {
    //获取环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.enableCheckpointing(5000)

    //构造kafka消费者
    val properties = new Properties()
    properties.setProperty("bootstrap.servers",KAFKA_BROKER)
    properties.setProperty("zookeeper.connect",ZOOKEEPER_HOST)
    properties.setProperty("group.id",TRANSACTION_GROUP)

    //获取数据

  }

}
