package org.haoxin.bigdata.flink.sql


import org.apache.flink.table.api.{Table, TableEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.scala._

/**
  * @author huolibin@haoxin.cn
  * @date Created by sheting on 2019/6/28 16:59
  *
  */
object sql_test {

  def main(args: Array[String]): Unit = {
    //获取执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //获取table
    val tableEnv = TableEnvironment.getTableEnvironment(env)
    //读取数据
   val source1: DataStream[String] = env.readTextFile("E:/ceshi/person.txt")
    println("------------------------------------")
    source1.print()
    println("------------------------------------")
    val source2: DataStream[Person1] = source1.map(x => {
      val split = x.split(" ")
      (Person1(split(0), split(1)))
    })
    //将datastream转化成table
    val table1: Table = tableEnv.fromDataStream(source2)
    //注册表
    tableEnv.registerTable("person",table1)
    //获取表中所有信息
    val rs: Table = tableEnv.sqlQuery("select * from person")

    val result = rs.select("name").toAppendStream[String] //输出name列
    val result1 = rs.toAppendStream[Person1] //输出所有

    result1.print()
    env.execute("filnksql_test")

  }
}

case class Person1(name:String, sex:String)