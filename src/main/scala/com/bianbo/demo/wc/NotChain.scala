package com.bianbo.demo.wc

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

object NotChain {
  def main(args: Array[String]): Unit = {
    //创建流处理执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
//    env.disableOperatorChaining() 全局禁用 operator chain

    // 从程序运行参数中读取hostname和port
    val params: ParameterTool = ParameterTool.fromArgs(args)
    val hostname: String = params.get("host")
    val port: Int = params.getInt("port")
    // 接受socket文本流，nc -lk 8888
    val inputDataStream: DataStream[String] = env.socketTextStream(hostname, port)

    // 定义转换操作，word count
    val resultDataStream: DataStream[(String, Int)] = inputDataStream
      .flatMap(_.split(" "))
      .filter(_.nonEmpty).disableChaining() // 此算子子任务将不会和前后算子进行 operator chain
      .map( (_, 1) ).startNewChain() // 从此算子处开始一个新的chain，所以只断前面，不断后面
      .keyBy(0)
      .sum(1)

    resultDataStream.print()

    env.execute("stream word count job")
  }
}
