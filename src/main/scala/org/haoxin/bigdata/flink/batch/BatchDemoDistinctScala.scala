package org.haoxin.bigdata.flink.batch

import org.apache.flink.api.scala.ExecutionEnvironment

import scala.collection.mutable.ListBuffer

/**
  * @author huolibin@haoxin.cn
  * @date Created by sheting on 2019/8/1 14:43
  *
  */
object BatchDemoDistinctScala {
  def main(args: Array[String]): Unit = {

    //获取环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    val data = ListBuffer[String]()
    data.append("hello you")
    data.append("hello me")

    import org.apache.flink.api.scala._
    val text = env.fromCollection(data)

   val flatData = text.flatMap(_.split("\\W+"))

    flatData.distinct().print()

  }

}
