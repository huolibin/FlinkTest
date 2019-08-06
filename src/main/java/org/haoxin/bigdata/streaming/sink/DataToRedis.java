package org.haoxin.bigdata.streaming.sink;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2019/7/19 15:55
 * 接收scoket数据，把数据保存到redis
 *
 */
public class DataToRedis {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> text = env.socketTextStream("192.168.71.9", 8902, "\n");

        //对数据进行组装，把string转化为tuple2<String,String>
        DataStream<Tuple2<String, String>> l_wordsdata = text.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String value) throws Exception {
                return new Tuple2<>("l_words", value);
            }
        });

        //创建redis的配置
        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("192.168.71.9").setPort(6379).build();

        //创建redis sink
        RedisSink<Tuple2<String, String>> redisSink = new RedisSink<>(conf, new MyRedisMapper());

        l_wordsdata.addSink(redisSink);

        env.execute("DataToRedis");

    }

    public static class MyRedisMapper implements RedisMapper<Tuple2<String, String>> {

        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.LPUSH);
        }

        //表示从接收的数据中获取需要操作的redis key
        @Override
        public String getKeyFromData(Tuple2<String, String> data) {
            return data.f0;
        }

        //表示从接收的数据中获取需要操作的redis value
        @Override
        public String getValueFromData(Tuple2<String, String> data) {
            return data.f1;
        }
    }

}
