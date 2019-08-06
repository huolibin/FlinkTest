package org.haoxin.bigdata.flink.streaming.sink

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

/**
  * @author huolibin@haoxin.cn
  * @date Created by sheting on 2019/7/31 14:39
  *
  */
object DataToRediScala {
  def main(args: Array[String]): Unit = {

    //socket端口
    val port = 8902

    //获取环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //获取数据
    val text = env.socketTextStream("192.168.71.9",port,'\n')

    //隐式转换
    import org.apache.flink.api.scala._
    //数据处理
    val l_wordsData = text.map(line =>("l_words_scala",line))

    //redis配置
    val conf = new FlinkJedisPoolConfig.Builder().setHost("192.168.71.9").setPort(6379).build()
    val redisSink = new RedisSink[Tuple2[String,String]](conf,new MyRedisMapperScala)

    l_wordsData.addSink(redisSink)

    //执行任务
    env.execute("DataToRediScala")

  }

  class MyRedisMapperScala extends RedisMapper[Tuple2[String,String]]{
    override def getCommandDescription: RedisCommandDescription = {
      new RedisCommandDescription(RedisCommand.LPUSH)
    }

    override def getKeyFromData(t: (String, String)): String = {
      t._1
    }

    override def getValueFromData(t: (String, String)): String = {
      t._2
    }
  }

}
