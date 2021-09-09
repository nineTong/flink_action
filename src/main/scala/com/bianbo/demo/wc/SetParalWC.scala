package com.bianbo.demo.wc

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

object SetParalWC {
  def main(args: Array[String]): Unit = {
    //创建流处理执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //  env.setParallelism(8) 设置所有算子任务并行度为8，同时也可以每个算子单独设置该参数

    // 从程序运行参数中读取hostname和port
    val params: ParameterTool = ParameterTool.fromArgs(args)
    val hostname: String = params.get("host")
    val port: Int = params.getInt("port")
    // 接受socket文本流，nc -lk 8888
    val inputDataStream: DataStream[String] = env.socketTextStream(hostname, port)

    // 定义转换操作，word count
    val resultDataStream: DataStream[(String, Int)] = inputDataStream
      .flatMap(_.split(" "))   // 以空格分词，打散得到所有的word
      .filter(_.nonEmpty)
      .map( (_, 1) )  // 转换成(word, count)二元组
      .keyBy(0)  // 按照第一个元素分组，keyBy算子不能设置并行度操作，首先它不是计算算子，它只是一个hash提示，是一种定义数据传输方式的算子,
      // 其次后续的aggregate算子
      // 都是基于keyedStream实现的
      .sum(1)  // 按照第二个元素求和，这里就能体现出有状态的流处理，结果是累计滚动输出的

    resultDataStream.print()
    // 如果在本地运行任务没有设定并行度，那么并行度默认与核数一致
    /**
     * 3> (world,1)
     * 2> (hello,1)
     * 4> (flink,1)
     * 2> (hello,2)
     * 1> (spark,1)
     * 2> (hello,3)
     * 3> (future,1)
     * 4> (stream,1)
     * 4> (is,1)
     * 4> (why,1)
     * 这里的数字代表的就是数据keyBy后的子任务序号
     */
    // resultDataStream.print().setParallelism(1)
    env.execute("stream word count job")
  }

}
