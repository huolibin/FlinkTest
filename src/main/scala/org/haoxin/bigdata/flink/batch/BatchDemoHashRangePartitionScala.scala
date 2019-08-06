package org.haoxin.bigdata.flink.batch

import org.apache.flink.api.scala.ExecutionEnvironment

import scala.collection.mutable.ListBuffer

/**
  * @author huolibin@haoxin.cn
  * @date Created by sheting on 2019/8/1 18:18
  *
  */
object BatchDemoHashRangePartitionScala {

  def main(args: Array[String]): Unit = {

    //获取环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._

    val data1 = ListBuffer[Tuple2[Int,String]]()
    data1.append((1,"hello1"))
    data1.append((2,"hello2"))
    data1.append((2,"hello3"))
    data1.append((3,"hello4"))
    data1.append((3,"hello5"))
    data1.append((3,"hello6"))
    data1.append((4,"hello7"))
    data1.append((4,"hello8"))
    data1.append((4,"hello9"))
    data1.append((4,"hello10"))
    data1.append((5,"hello11"))
    data1.append((5,"hello12"))
    data1.append((5,"hello13"))
    data1.append((5,"hello14"))
    data1.append((5,"hello15"))
    data1.append((6,"hello16"))
    data1.append((6,"hello17"))
    data1.append((6,"hello18"))
    data1.append((6,"hello19"))
    data1.append((6,"hello20"))
    data1.append((6,"hello21"))

    val text = env.fromCollection(data1)

    text.partitionByRange(0).mapPartition(it =>{
      while (it.hasNext){
        val line = it.next()
        println("当前线程id："+Thread.currentThread().getId+","+line)
      }
      it
    }).print()


    println("==============================================")
    text.partitionByHash(0).mapPartition(it =>{
      while (it.hasNext){
        val line = it.next()
        println("当前线程id："+Thread.currentThread().getId+","+line)
      }
      it
    }).print()



  }
}
