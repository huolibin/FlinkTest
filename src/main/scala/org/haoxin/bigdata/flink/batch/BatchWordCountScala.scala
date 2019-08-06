package org.haoxin.bigdata.flink.batch

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.api.scala._
/**
  * @author huolibin@haoxin.cn
  * @date Created by sheting on 2019/6/27 13:55
  *离线wordcount
  */
object BatchWordCountScala {
  def main(args: Array[String]): Unit = {

    //初始化环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    //加载数据
    //val text: DataSet[String] = env.fromElements("who's there?","I think I hear them. Stand,o! who's there?")
    val text = env.readTextFile("E:\\ceshi\\words.txt")
    //整理数据
    val counts: AggregateDataSet[(String, Int)] = text.flatMap(_.toLowerCase.split("\\W+")filter(_.nonEmpty) )
      .map((_, 1))
      .groupBy(0)
      .sum(1)
    //打印
   // counts.print()
    //保存数据
    //counts.writeAsCsv("E:/ceshi/out_word2/","\n",",").setParallelism(1)
    counts.writeAsCsv("E:/ceshi/out_word2").setParallelism(1)

    env.execute("BatchWordCountScala")
  }
}
