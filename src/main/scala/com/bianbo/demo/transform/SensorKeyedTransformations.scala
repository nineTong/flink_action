package com.bianbo.demo.transform

import com.bianbo.demo.source.SensorSource
import com.bianbo.demo.util.{SensorReading, SensorTimeAssigner}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._

object SensorKeyedTransformations {
  /** main() defines and executes the DataStream program */
  def main(args: Array[String]) {

    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // use event time for the application
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // configure watermark interval
    env.getConfig.setAutoWatermarkInterval(1000L)

    // ingest sensor stream
    val readings: DataStream[SensorReading] = env
      // SensorSource generates random temperature readings
      .addSource(new SensorSource)
      // assign timestamps and watermarks which are required for event time
      .assignTimestampsAndWatermarks(new SensorTimeAssigner)

    // group sensor readings by their id
    val keyed: KeyedStream[SensorReading, String] = readings
        .keyBy(new MyKeySelector)
//      .keyBy(_.id)
//      .keyBy("id") 注意此方法返回的key的类型为JavaTuple，因为不知道传入了几个字段

    // a rolling reduce that computes the highest temperature of each sensor and
    // the corresponding timestamp
    val maxTempPerSensor: DataStream[SensorReading] = keyed
      .reduce((r1, r2) => {
        if (r1.temperature > r2.temperature) r1 else r2
      })

    // reduce算子在执行时，KeyedStream的分区是多少，slot又是多少？
    // 并不是一个key一个单独分区，分区数量和slot数量一致，但是keyBy后的所有操作，在有状态的流计算中总能找到当前正在处理record的key所对应的状态
    // 可以自定义 org.apache.flink.api.common.functions.AggregateFunction实现，但是KeyedStream的aggregate函数是私有的，无法调用

    maxTempPerSensor.print()

    /**
     * 1> SensorReading(sensor_23,1626081170244,47.34042309934114)
     * 1> SensorReading(sensor_23,1626081170393,47.927379078108615)
     * 1> SensorReading(sensor_23,1626081170393,47.927379078108615)
     * 1> SensorReading(sensor_23,1626081170595,48.082690002759925)
     * 1> SensorReading(sensor_23,1626081170696,48.15642887714527)
     * 1> SensorReading(sensor_23,1626081170796,48.54895223342432)
     * 1> SensorReading(sensor_23,1626081170897,49.43123600401445)
     */
    // execute application
    env.execute("Keyed Transformations Example")
  }
}
