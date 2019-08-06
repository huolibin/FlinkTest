package org.haoxin.bigdata.flink.streaming.customSource

import org.apache.flink.streaming.api.functions.source.{ParallelSourceFunction, SourceFunction}

/**
  * @author huolibin@haoxin.cn
  * @date Created by sheting on 2019/7/30 17:09
  *  创建自定义多并行度的source
  *  实现从1 开始产生递增数字
  *
  */
class MyParallelSourceScala extends  ParallelSourceFunction[Long]{
  var count =1L
  var isRunning = true
  override def run(ctx: SourceFunction.SourceContext[Long]): Unit = {
    while (isRunning){
      ctx.collect(count)
      count +=1
      Thread.sleep(1000)
    }
  }

  override def cancel(): Unit = {
    isRunning = false
  }
}
