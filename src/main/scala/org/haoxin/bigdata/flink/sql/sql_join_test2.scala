package org.haoxin.bigdata.flink.sql

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.{Table, TableEnvironment}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._

/**
  * @author huolibin@haoxin.cn
  * @date Created by sheting on 2019/6/28 18:17
  *
  *stream sql_join
  */
object sql_join_test2 {
  def main(args: Array[String]): Unit = {
    //获取环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv: StreamTableEnvironment = TableEnvironment.getTableEnvironment(env)

    //读取数据源
    val source1: DataStream[String] = env.readTextFile("E:/ceshi/flink_data/scoket1.txt")
    val source2: DataStream[String] = env.readTextFile("E:/ceshi/flink_data/scoket2.txt")

    //整理数据
    val dataSource1: DataStream[TextData3] = source1.map(x => {
      val splits = x.split(",")
      TextData3(splits(0), splits(1), splits(2).toDouble)
    })

    val dataSource2: DataStream[TextData4] = source2.map(x => {
      val splits = x.split(",")
      TextData4(splits(0), splits(1), splits(2).toDouble)
    })

    //datastream转换成table
    val table1 = tableEnv.fromDataStream(dataSource1)
    val table2 = tableEnv.fromDataStream(dataSource2)

    //注册table
    tableEnv.registerTable("data_test1",table1)
    tableEnv.registerTable("data_test2",table2)

    //sql查询
    val sql = "select tx_time,tx_code,tx_value from data_test1 as a join data_test2 as b on a.tx_code =b.md_code"
    val rs: Table = tableEnv.sqlQuery(sql)

   // val rs: Table = tableEnv.sqlQuery("select * from data_test1")
    //val value = rs.select("tx_time").toAppendStream[String]
    val result = rs.toAppendStream[(String,String,Double)]
    result.writeAsCsv("E:/ceshi/flink_data/testcvs")
    result.print()
    env.execute("filnksql_test2")
  }

}
case class TextData3(tx_time:String,tx_code:String,tx_value:Double)
case class TextData4(md_time:String,md_code:String,md_value:Double)