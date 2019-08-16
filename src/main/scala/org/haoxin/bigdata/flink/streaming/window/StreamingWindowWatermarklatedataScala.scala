package org.haoxin.bigdata.flink.streaming.window

import java.text.SimpleDateFormat

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ArrayBuffer
import scala.util.Sorting


/**
  * watermark scala的  单并行度下的应用
  * sideOutputLateData保存迟到的数据
  *
  * @author huolibin@haoxin.cn
  * @date Created by sheting on 2019/8/15 17:24
  *
  */
object StreamingWindowWatermarklatedataScala {
  def main(args: Array[String]): Unit = {
    val port =8902
    val hostname = "192.168.71.10"
    val splitname = "\n"

    //获取flink运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._

    //设置使用event-time
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    //获取socket环境
    val text: DataStream[String] = env.socketTextStream(hostname,port,'\n')

    //解析数据
    val mapData = text.map(line => {
      val arr = line.split(",")
      (arr(0), arr(1).toLong)
    })

    //对数据 timestamp和watermark
    val markData = mapData.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[(String, Long)] {
      var currentMaxTimestamp = 0L //当前最大时间戳
      var maxOutOforderness = 10000L //最大允许的乱序时间是10s
      private val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

      override def getCurrentWatermark: Watermark = {
        new Watermark(currentMaxTimestamp - maxOutOforderness)
      }

      override def extractTimestamp(element: (String, Long), previousElementTimestamp: Long): Long = {
        val timestamp = element._2
        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp)
        val id = Thread.currentThread().getId
        println("currentThreadId:" + id + ",key:" + element._1 + ",eventtime:[" + element._2 + "|" + sdf.format(element._2) + "],currentMaxTimestamp:[" + currentMaxTimestamp + "|" + sdf.format(currentMaxTimestamp) + "],watermark:[" + getCurrentWatermark().getTimestamp + "|" + sdf.format(getCurrentWatermark().getTimestamp) + "]")
        timestamp
      }
    })

    //保存被丢弃的数据
    val outputTag = new OutputTag[Tuple2[String, Long]]("lated-data"){}
    //计算
    val windowData = markData.keyBy(0).timeWindow(Time.seconds(3))
      .sideOutputLateData(outputTag)  //保存超时数据
      .apply(new WindowFunction[Tuple2[String, Long], String, Tuple, TimeWindow] {
        override def apply(key: Tuple, window: TimeWindow, input: Iterable[(String, Long)], out: Collector[String]): Unit = {
          val arrBuf = ArrayBuffer[Long]()
          val it = input.iterator
          while (it.hasNext) {
            val tup2 = it.next()
            arrBuf.append(tup2._2)
          }
          val arr = arrBuf.toArray
          Sorting.quickSort(arr) //排序

          val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
          val result = key.toString + "," + arr.length + "," + sdf.format(arr.head) + "," + sdf.format(arr.last) + "," + sdf.format(window.getStart) + "," + sdf.format(window.getEnd)

          out.collect(result)

        }
      })
    //把迟到的数据打印到控制台，生产中保存在redis等
    val sideOut = windowData.getSideOutput(outputTag)
    sideOut.print()

    //打印
    windowData.print()

    env.execute("StreamingWindowWatermarklatedataScala")


  }

}
