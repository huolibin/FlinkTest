package org.haoxin.bigdata.flink.streaming

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * @author huolibin@haoxin.cn
  * @date Created by sheting on 2019/6/26 18:03
  *flink测试
  */

object SocketWindowWordCountScala {
  def main(args: Array[String]): Unit = {

    //port 表示需要连接的端口
    val port: Int = try {
      ParameterTool.fromArgs(args).getInt("port")
    } catch {
      case e: Exception => {
        System.err.println("No port set. use default port 9000")
        9000
      }
    }

        //获取环境
        val env = StreamExecutionEnvironment.getExecutionEnvironment

        //获取数据
        val text = env.socketTextStream("192.168.71.9", port)

        //隐式转换
        import org.apache.flink.api.scala._

        //    解析数据，分组，窗口化，并且聚合
        val windowvounts: DataStream[WordWithCount] = text.flatMap { w => w.split("\\s") }
          .map { w => WordWithCount(w, 1) }
          .keyBy("word")
          .timeWindow(Time.seconds(5), Time.seconds(1))
          .sum("count")

    windowvounts.print().setParallelism(1)
    env.execute("SocketWindowWordCountScala")

    }
    //定义一个数据类型保存单词出现的次数
    case class WordWithCount(word: String, count: Long)


}
