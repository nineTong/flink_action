package com.bianbo.demo.transform

import com.bianbo.demo.source.SensorSource
import com.bianbo.demo.transform.util.SmokeLevel.SmokeLevel
import com.bianbo.demo.transform.util.{Alert, SmokeLevel, SmokeLevelSource}
import com.bianbo.demo.util.{SensorReading, SensorTimeAssigner}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
  * A simple application that outputs an alert whenever there is a high risk of fire.
  * The application receives the stream of temperature sensor readings and a stream of smoke level measurements.
  * When the temperature is over a given threshold and the smoke level is high, we emit a fire alert.
  * connect操作是split操作的反向操作，connect同样是将两条流逻辑上整合在一起，但并没有对整合过后的数据流进行处理，两条流中数据类型并没有改变
  * 如果需要处理，那么就需要结合 Comap Coflatmap 进行操作
  */
object MultiStreamTransformations {

  /** main() defines and executes the DataStream program */
  def main(args: Array[String]) {

    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // use event time for the application
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // configure watermark interval
    env.getConfig.setAutoWatermarkInterval(1000L)

    // ingest sensor stream
    val tempReadings: DataStream[SensorReading] = env
      // SensorSource generates random temperature readings
      .addSource(new SensorSource)
      // assign timestamps and watermarks which are required for event time
      .assignTimestampsAndWatermarks(new SensorTimeAssigner)

    // ingest smoke level stream
    val smokeReadings: DataStream[SmokeLevel] = env
      .addSource(new SmokeLevelSource)
      .setParallelism(1)

    // group sensor readings by their id
    val keyed: KeyedStream[SensorReading, String] = tempReadings
      .keyBy(_.id)

    // connect the two streams and raise an alert if the temperature and smoke levels are high，注意数据类型
    var value: ConnectedStreams[SensorReading, SmokeLevel] = keyed
      .connect(smokeReadings.broadcast)

    var alerts: DataStream[Alert] = value
      .flatMap(new RaiseAlertFlatMap)

    alerts.print()

    // execute application
    env.execute("Multi-Stream Transformations Example")
  }

  /**
    * A CoFlatMapFunction that processes a stream of temperature readings ans a control stream
    * of smoke level events. The control stream updates a shared variable with the current smoke level.
    * For every event in the sensor stream, if the temperature reading is above 100 degrees
    * and the smoke level is high, a "Risk of fire" alert is generated.
    */
  class RaiseAlertFlatMap extends CoFlatMapFunction[SensorReading, SmokeLevel, Alert] {

    var smokeLevel = SmokeLevel.Low

    override def flatMap1(in1: SensorReading, collector: Collector[Alert]): Unit = {
      // high chance of fire => true
      if (smokeLevel.equals(SmokeLevel.High) && in1.temperature > 50) {
        collector.collect(Alert("Risk of fire!", in1.timestamp))
      }
    }

    override def flatMap2(in2: SmokeLevel, collector: Collector[Alert]): Unit = {
      // 在执行此方法时是否有同步锁，是否能保证该方法先执行
      smokeLevel = in2
    }
  }
}
