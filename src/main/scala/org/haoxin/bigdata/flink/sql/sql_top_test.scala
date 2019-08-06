package org.haoxin.bigdata.flink.sql

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.table.api.{Table, TableEnvironment}
import org.apache.flink.table.api.scala.BatchTableEnvironment
//import org.apache.flink.table.api.scala._
import org.apache.flink.api.scala._
/**
  * @author huolibin@haoxin.cn
  * @date Created by sheting on 2019/7/2 15:18
  *
  * 求topn问题,windows/linux上测试已成功
  *
  */
object sql_top_test {
  def main(args: Array[String]): Unit = {
    //获取环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv: BatchTableEnvironment = TableEnvironment.getTableEnvironment(env)
    //读取数据
    //val source: DataSet[String] = env.readTextFile("E:/ceshi/flink_data/topn.txt")
    val source: DataSet[String] = env.readTextFile("/root/test/flink_test/test1")
    source.print()
    println("-------------------- start --------------------")

    val text: DataSet[PlayData] = source.map(x => {
      val str = x.split(",")
      PlayData(str(0).trim().toInt, str(1).trim(), str(2).trim().toInt, str(3).trim().toDouble)
    })


    //注册table
    val topScore: Table = tableEnv.fromDataSet(text)
    tableEnv.registerTable("score",topScore)

    //编写sql
    val sql="select name, count(id) as num from score group by name order by num desc"
    val queryResult: Table = tableEnv.sqlQuery(sql)

    //打印
    val result: DataSet[ResultData] = tableEnv.toDataSet[ResultData](queryResult)
    result.print()
    println("-------------------- end --------------------")
    //env.execute("sql_top_test")
  }

}
case class PlayData(id:Int,name:String,code:Int,scode:Double)
case class ResultData(name:String, num:Long);
