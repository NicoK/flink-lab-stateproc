package com.ververica.stateproc

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._

object FlinkJob {
  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    env.fromElements(
      Account(1, 1.0, 100L),
      Account(2, 1.0, 102L),
      Account(3, 1.0, 103L),
      Account(4, 1.0, 104L),
    )
      .keyBy(_.id)
      .map(new MyMap)
      .uid("uid1")
      .print()

    env.execute("Flink job")
  }

  class MyMap extends RichMapFunction[Account, Tuple2[Int, Double]] {
    var state: ValueState[Account] = _

    override def open(parameters: Configuration): Unit = {
      val descriptor = new ValueStateDescriptor[Account]("previous", createTypeInformation[Account])
      state = getRuntimeContext.getState(descriptor)
    }

    override def map(value: Account): (Int, Double) = {
      val previous = state.value()
      if (previous != null) {
        Tuple2(value.id, (value.amount + previous.amount) / 2.0)
      } else {
        Tuple2(value.id, value.amount)
      }
    }
  }
}
