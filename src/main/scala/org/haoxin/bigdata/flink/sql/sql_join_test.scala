package org.haoxin.bigdata.flink.sql

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.table.api.{Table, TableEnvironment}
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.apache.flink.api.scala._

/**
  * @author huolibin@haoxin.cn
  * @date Created by sheting on 2019/6/28 18:17
  * 离散数据batch jion
  */
object sql_join_test {
  def main(args: Array[String]): Unit = {
    //获取环境
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv: BatchTableEnvironment = TableEnvironment.getTableEnvironment(env)

    //读取数据源
    val source1: DataSet[String] = env.readTextFile("E:/ceshi/flink_data/scoket1.txt")
    val source2: DataSet[String] = env.readTextFile("E:/ceshi/flink_data/scoket2.txt")

    println("----------------------start print text-----------------------")
    source1.print()
    source2.print()

    //整理数据
    val dataSource1: DataSet[TextData1] = source1.map(x => {
      val splits = x.split(",")
      TextData1(splits(0), splits(1), splits(2).toDouble)
    })
//    val dataSource1 = source1.map(x => {
//      val splits = x.split(",")
//      (splits(0), splits(1), splits(2).toDouble)
//    })

    val dataSource2: DataSet[TextData2] = source2.map(x => {
      val splits = x.split(",")
      TextData2(splits(0), splits(1), splits(2).toDouble)
    })


    /**
      * 方法一：注册registerDataSet
      */

//    tableEnv.registerDataSet("flink_test1",dataSource1)
//    tableEnv.registerDataSet("flink_test2",dataSource2)

    /**
      * 方法二：先转换成table，再注册registertable
      */
              //dataset转换成table
    val table1 = tableEnv.fromDataSet(dataSource1)
    val table2 = tableEnv.fromDataSet(dataSource2)

               //注册table
    tableEnv.registerTable("flink_test1",table1)
    tableEnv.registerTable("flink_test2",table2)

    //sql查询
    val sql ="select tx_time,tx_code,tx_value,md_time,md_code,md_value from flink_test1 as a join flink_test2 as b on a.tx_code =b.md_code"
    val rs: Table = tableEnv.sqlQuery(sql)
    val value: DataSet[QueryResult] = tableEnv.toDataSet[QueryResult](rs)
    //打印
    println("---------end---------------------")
    value.print()
    //env.execute("filnksql_test1")
  }

}
case class TextData1(tx_time:String,tx_code:String,tx_value:Double)
case class TextData2(md_time:String,md_code:String,md_value:Double)
case class QueryResult(a:String,b:String,c:Double,d:String,e:String,f:Double)