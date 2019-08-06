package org.haoxin.bigdata.flink.batch

import org.apache.flink.api.scala.ExecutionEnvironment

import scala.collection.mutable.ListBuffer

/**
  * outerjoin外连接
  * join中 map和apply的效果是一样的
  *
  * @author huolibin@haoxin.cn
  * @date Created by sheting on 2019/8/1 14:43
  *
  */
object BatchDemoOuterJoinScala {
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

    println("======左连接====")
    text1.leftOuterJoin(text2).where(0).equalTo(0).apply((first, second) => {
      if (second == null){
        (first._1, first._2, "null")
      }else{
        (first._1, first._2, second._2)
      }

    }).print()


    println("======右连接====")
    text1.rightOuterJoin(text2).where(0).equalTo(0).apply((first, second) => {
      if (first == null){
        (second._1,  "null", second._2)
      }else{
        (first._1, first._2, second._2)
      }

    }).print()

    println("======全连接====")

    text1.fullOuterJoin(text2).where(0).equalTo(0).apply((first, second) => {
      if (first == null){
        (second._1,  "null", second._2)
      }else if (second == null){
        (first._1, first._2, "null")
      } else{
        (first._1, first._2, second._2)
      }

    }).print()

  }

}
