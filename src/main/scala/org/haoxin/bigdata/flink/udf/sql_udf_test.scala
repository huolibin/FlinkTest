package org.haoxin.bigdata.flink.udf

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.table.functions.ScalarFunction

/**
  * @author huolibin@haoxin.cn
  * @date Created by sheting on 2019/7/3 14:57
  *
  */
object sql_udf_test {

  def main(args: Array[String]): Unit = {
    //获取环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv: BatchTableEnvironment = TableEnvironment.getTableEnvironment(env)

    //读取数据，整理数据
    val readData: DataSet[String] = env.readTextFile("")
    val textOne: DataSet[(String, Int)] = readData.flatMap(_.split(" ")).map((_,1))

    //注册表


    //注册udf

    //编写SQL语句


    //打印结果
  }

}
case class HashCode() extends ScalarFunction{

}