package org.haoxin.bigdata.flink.streaming.customPatition

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.haoxin.bigdata.flink.streaming.customSource.MyNoParallelSourceScala

/**
  * @author huolibin@haoxin.cn
  * @date Created by sheting on 2019/7/30 18:28
  *
  */
object demoMypartitionScala {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //限定分区总数
    env.setParallelism(3)
    //隐式转换
    import  org.apache.flink.api.scala._
    val text = env.addSource(new MyNoParallelSourceScala)

    //把long类型的数据转化成tuple类型
    val tupleData: DataStream[Tuple1[Long]] = text.map(line => {
      Tuple1(line)
    })
    val partitionData = tupleData.partitionCustom(new MyPartitionerScala,0)

    val result = partitionData.map(line => {
      println("当前线程id:" + Thread.currentThread().getId + ",value" + line)
      line._1
    })
    result.print().setParallelism(1)

    env.execute("demoMypartitionScala")

  }

}
