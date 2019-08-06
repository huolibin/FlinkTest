package org.haoxin.bigdata.flink.streaming.customSource

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * @author huolibin@haoxin.cn
  * @date Created by sheting on 2019/7/30 17:13
  *
  */
object demo1 {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //隐式转换
    import org.apache.flink.streaming.api.scala._
    val text: DataStream[Long] = env.addSource(new MyNoParallelSourceScala)
    //数据处理
    val mapData = text.map(line => {
      println("接收到的数据:" + line)
      line
    })

    val sum = mapData.timeWindowAll(Time.seconds(5)).sum(0)

    sum.print().setParallelism(1);
    env.execute("demo1")
  }

}
