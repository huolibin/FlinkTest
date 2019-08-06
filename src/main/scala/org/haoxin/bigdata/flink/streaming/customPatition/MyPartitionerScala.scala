package org.haoxin.bigdata.flink.streaming.customPatition

import org.apache.flink.api.common.functions.Partitioner

/**
  * @author huolibin@haoxin.cn
  * @date Created by sheting on 2019/7/30 18:25
  *
  */
class MyPartitionerScala  extends Partitioner[Long]{
  override def partition(key: Long, numPartitions: Int): Int = {
    println("分区总数："+numPartitions)
    if(key % 2 ==0){
      0
    }else{
      1
    }
  }
}
