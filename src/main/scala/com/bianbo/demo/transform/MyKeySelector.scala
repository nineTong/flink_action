package com.bianbo.demo.transform

import com.bianbo.demo.util.SensorReading
import org.apache.flink.api.java.functions.KeySelector

class MyKeySelector extends KeySelector[SensorReading, String]{
  override def getKey(value: SensorReading): String = value.id
}
