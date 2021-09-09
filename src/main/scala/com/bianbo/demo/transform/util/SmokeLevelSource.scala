package com.bianbo.demo.transform.util

import com.bianbo.demo.transform.util.SmokeLevel.SmokeLevel
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext

import scala.util.Random

/**
  * Flink SourceFunction to generate random SmokeLevel events.
  */
class SmokeLevelSource extends RichParallelSourceFunction[SmokeLevel] {

  // flag indicating whether source is still running.
  var running: Boolean = true

  /** run() continuously emits SmokeLevel events by emitting them through the SourceContext. */
  override def run(srcCtx: SourceContext[SmokeLevel]): Unit = {

    // initialize random number generator
    val rand = new Random()

    // emit data until being canceled
    while (running) {

      if (rand.nextGaussian() > 0.8 ) {
        // emit a high SmokeLevel
        srcCtx.collect(SmokeLevel.High)
      }
      else {
        srcCtx.collect(SmokeLevel.Low)
      }

      // wait for 1s
      Thread.sleep(1000)
    }

  }

  /** Cancels this SourceFunction. */
  override def cancel(): Unit = {
    running = false
  }

}
