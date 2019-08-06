package org.haoxin.bigdata.flink.streaming

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * @author huolibin@haoxin.cn
  * @date Created by sheting on 2019/7/30 16:31
  *
  */
object StreamFromCollect {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val data = List(10,15,20)
    //隐式转换
    import org.apache.flink.streaming.api.scala._

    val text = env.fromCollection(data)
    //对数据处理,加1
    val num = text.map(_+1)
    num.print().setParallelism(1)

    env.execute("StreamFromCollect")

  }

}
