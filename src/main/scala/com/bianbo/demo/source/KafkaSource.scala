package com.bianbo.demo.source

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

object KafkaSource {
  def main(args: Array[String]): Unit = {
    // 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val props = new Properties
    props.put("bootstrap.servers", "192.168.132.6:9092")
//    props.put("zookeeper.connect", "192.168.132.6:2181")
    props.put("group.id", "metrics-group")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer") //key 反序列化

    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer") //value 反序列化

    props.put("auto.offset.reset", "latest")

    // 可以查看FlinkKafkaConsumer011类图，该类同样继承了RichParallelSourceFunction接口
    val dataStreamSource = env.addSource(new FlinkKafkaConsumer011[String]("metrics", //kafka topic
      new SimpleStringSchema, // String 序列化
      props)).setParallelism(1)

    dataStreamSource.print //把从 kafka 读取到的数据打印在控制台

    env.execute("Flink add data source")
  }
}


