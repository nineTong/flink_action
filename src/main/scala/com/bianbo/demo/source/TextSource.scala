package com.bianbo.demo.source

import org.apache.flink.streaming.api.scala._

object TextSource {
  def main(args: Array[String]): Unit = {
    // 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 从文件中读取数据
     val stream: DataStream[String] = env.readTextFile("/root/IdeaProjects/flinkDemo/src/main/resources/sensor.txt")

    /**
     * 流式读取文件和批处理读取文件的区别：
     * 批处理读取文件时，是将文件分块后一次性读取，然后同时处理所有记录，最后输出一个结果
     * 流式读取文件是将文件中数据逐条读取，然后将数据逐条处理，处理结果滚动更新，体现中间状态
     */
    stream.print("stream")
    env.execute("text source test job")
  }
}


