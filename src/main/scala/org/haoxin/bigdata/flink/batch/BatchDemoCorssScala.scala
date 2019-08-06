package org.haoxin.bigdata.flink.batch

import org.apache.flink.api.scala.ExecutionEnvironment

import scala.collection.mutable.ListBuffer

/**
  * @author huolibin@haoxin.cn
  * @date Created by sheting on 2019/8/1 16:18
  *
  */
object BatchDemoCorssScala {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val data1 = ListBuffer[String]()
    data1.append("hello")
    data1.append("you")
    data1.append("me")

    val data2 = ListBuffer[Int]()
    data2.append(1)
    data2.append(2)

    import org.apache.flink.api.scala._

    val text1 = env.fromCollection(data1)
    val text2 = env.fromCollection(data2)

    val crossData = text1.cross(text2)
    crossData.print()

  }

}
