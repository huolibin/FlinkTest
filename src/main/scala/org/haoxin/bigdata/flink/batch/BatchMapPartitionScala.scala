package org.haoxin.bigdata.flink.batch

import org.apache.flink.api.scala.ExecutionEnvironment

import scala.collection.mutable.ListBuffer

/**
  * @author huolibin@haoxin.cn
  * @date Created by sheting on 2019/8/1 14:43
  *
  */
object BatchMapPartitionScala {
  def main(args: Array[String]): Unit = {

    //获取环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    val data = ListBuffer[String]()
    data.append("hello you")
    data.append("hello me")

    import org.apache.flink.api.scala._
    val text = env.fromCollection(data)

    text.mapPartition(it =>{
     val res = ListBuffer[String]()
      while (it.hasNext){
        val line = it.next()
        val words = line.split("\\W+")
        for(w <- words){
          res.append(w)
        }
      }
      res
    }).print()

  }

}
