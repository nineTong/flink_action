package com.bianbo.demo.source

import com.bianbo.demo.util.SensorReading
import org.apache.flink.streaming.api.scala._

object SensorSourceTest {
  def main(args: Array[String]): Unit = {
    // 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream: DataStream[SensorReading] = env.addSource(new SensorSource)

    stream.print

    /**
     * 1> SensorReading(sensor_8,1626078634356,63.229261756404696)
     * 1> SensorReading(sensor_9,1626078634356,97.00638825901073)
     * 1> SensorReading(sensor_10,1626078634356,45.98178036850143)
     * 3> SensorReading(sensor_21,1626078634357,88.18152533293497)
     * 3> SensorReading(sensor_22,1626078634357,89.36534212073046)
     * 3> SensorReading(sensor_23,1626078634357,59.49823184670278)
     * 子任务编号从0开始
     */
    env.execute("SensorSource source test job")
  }
}
