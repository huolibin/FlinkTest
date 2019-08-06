package org.haoxin.bigdata.flink.batch

import org.apache.flink.api.scala.ExecutionEnvironment

import scala.collection.mutable.ListBuffer

/**
  * union的应用
  *
  *
  * @author huolibin@haoxin.cn
  * @date Created by sheting on 2019/8/1 14:43
  *
  */
object BatchDemoUnionScala {
  def main(args: Array[String]): Unit = {

    //获取环境
    val env = ExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._

    val data1 = ListBuffer[Tuple2[Int,String]]()
    data1.append((1,"zs"))
    data1.append((2,"ls"))
    data1.append((3,"ww"))

    val data2 = ListBuffer[Tuple2[Int,String]]()
    data2.append((1,"beijing"))
    data2.append((2,"shanghai"))
    data2.append((4,"guangzhou"))

    val text1: DataSet[(Int, String)] = env.fromCollection(data1)
    val text2: DataSet[(Int, String)] = env.fromCollection(data2)

    val unionData = text1.union(text2)

    unionData.print()



  }

}
