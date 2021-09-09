package com.bianbo.demo.transform

import com.bianbo.demo.source.SensorSource
import com.bianbo.demo.util.SensorReading
import org.apache.flink.streaming.api.scala._

object SplitAndSelect {
  /**
   * 这个算子是从逻辑上根据某些特征将一个流分成两个流，但在实际使用上这个流还没有被分开
   * 只是流中元素有了归属属性，还需要根据属性进行提取出来
   */
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val dataStream: DataStream[SensorReading] = env.addSource(new SensorSource)

    val splitStream: SplitStream[SensorReading] = dataStream
      .split( data => {
        if( data.temperature > 30 )
          Seq("high")
        else
          Seq("low")
      } )

    val highTempStream: DataStream[SensorReading] = splitStream.select("high")
    val lowTempStream: DataStream[SensorReading] = splitStream.select("low")
    val allTempStream: DataStream[SensorReading] = splitStream.select("high", "low")

    highTempStream.print
    env.execute("SensorSource source test job")
  }
}
