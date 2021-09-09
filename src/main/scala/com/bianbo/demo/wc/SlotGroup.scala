package com.bianbo.demo.wc

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

object SlotGroup {
  def main(args: Array[String]): Unit = {
    //创建流处理执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // 从程序运行参数中读取hostname和port
    val params: ParameterTool = ParameterTool.fromArgs(args)
    val hostname: String = params.get("host")
    val port: Int = params.getInt("port")
    // 接受socket文本流，nc -lk 8888
    val inputDataStream: DataStream[String] = env.socketTextStream(hostname, port)

    // 定义转换操作，word count
    val resultDataStream: DataStream[(String, Int)] = inputDataStream
      .flatMap(_.split(" ")).slotSharingGroup("1")
      .filter(_.nonEmpty).slotSharingGroup("2")
      .map( (_, 1) )
      .keyBy(0)
      .sum(1)

    /**
     * 没有设置slot组的算子默认组为group
     * 默认情况下只有同一组的算子才能共享slot，并且同一算子的不同子任务不共享slot
     * 所以以上代码socket算子并行度是1且slot组是default，需要1个slot
     * faltMap算子并行度为2，slot组是1，所以需要2个slot
     * filter算子并行度为2，slot组是2，所以需要2个slot
     * 以上算子各子任务均不能共享slot，所以需要五个slot
     * 在这种情况下，faltMap和filter不会进行operator chain操作
     */

    resultDataStream.print()

    env.execute("stream word count job")
  }
}
