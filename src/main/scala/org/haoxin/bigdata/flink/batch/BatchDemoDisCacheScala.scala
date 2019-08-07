package org.haoxin.bigdata.flink.batch

import java.io.File
import java.util

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration
import org.apache.commons.io.FileUtils

/**
  * distributed cache 分布式缓存
  *
  * @author huolibin@haoxin.cn
  * @date Created by sheting on 2019/8/7 10:35
  *
  */
object BatchDemoDisCacheScala {
  def main(args: Array[String]): Unit = {
    //获取运行环境
    val env = ExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._

    //1.注册一个文件
    env.registerCachedFile("e:\\ceshi\\files\\b.txt","cacheFile")

    //获取源数据
    val data: DataSet[String] = env.fromElements("a","b","c")

    //利用缓存文件处理源数据
    val mapData = data.map(new RichMapFunction[String, String] {
      override def open(parameters: Configuration): Unit = {
        super.open(parameters)
        //2.访问缓存数据
        val cacheFile: File = getRuntimeContext.getDistributedCache.getFile("cacheFile")
        val strings = FileUtils.readLines(cacheFile)
        val it = strings.iterator()
        while (it.hasNext) {
          val str = it.next()
          println("cache数据：" + str)
        }
      }

      override def map(value: String): String = {
        value
      }
    })

    //打印
    mapData.print()


  }
}
