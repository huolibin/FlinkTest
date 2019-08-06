package org.haoxin.bigdata.streaming.customSource.MysqlSource;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2019/7/17 16:18
 * 对自定义mysql source的测试
 */
public class demo1 {
    public static void main(String[] args) throws Exception {
        //获取环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //获取数据
        DataStreamSource<Tuple2<String, String>> text = env.addSource(new JdbcReader()).setParallelism(1);

        text.print().setParallelism(1);
        env.execute("demo1");

    }
}
