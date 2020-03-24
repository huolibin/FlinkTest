package org.haoxin.bigdata.streaming;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.TimeUnit;


/**
 * dataStream 重启策略
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2020/3/24 10:44
 */
public class RestarTest {
    public static void main(String[] args) {

        //获取flink的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //启动checkpoint,每1000ms启动一次
        env.enableCheckpointing(1000);

        //1.间隔重启：间隔10秒 重启3次
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(10)));

        //2.失败率重启：5分钟内若失败3次测认为该job失败，重试间隔为10s
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3,Time.of(5, TimeUnit.MINUTES),Time.of(10,TimeUnit.SECONDS)));

        //3.不重启
        env.setRestartStrategy(RestartStrategies.noRestart());
    }
}
