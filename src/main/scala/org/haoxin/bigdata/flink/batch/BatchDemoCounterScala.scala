package org.haoxin.bigdata.flink.batch

import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.api.common.accumulators.IntCounter
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration

/**
  *
  * counter 累加器
  * @author huolibin@haoxin.cn
  * @date Created by sheting on 2019/8/5 17:20
  *
  */
object BatchDemoCounterScala {
  def main(args: Array[String]): Unit = {

    //获取环境
    val env = ExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._

    //源数据
    val data = env.fromElements("a","b","s")

    val res = data.map(new RichMapFunction[String, String] {
      //1.创建累加器
      private val numLines = new IntCounter()

      override def open(parameters: Configuration): Unit = {
        super.open(parameters)
        //2.注册累加器
        getRuntimeContext.addAccumulator("num-lines-scala", numLines)
      }

      override def map(value: String): String = {
        this.numLines.add(1)
        value
      }
    }).setParallelism(4)


    res.writeAsText("e:\\ceshi\\count_scala")

    val jobResult: JobExecutionResult = env.execute("BatchDemoCounterScala")

    //3.获取累加器
    val num: Int = jobResult.getAccumulatorResult("num-lines-scala")

    println(num)


  }

}
