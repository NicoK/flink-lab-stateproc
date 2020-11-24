package com.ververica.stateproc

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.ExecutionEnvironment
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.state.api.functions.KeyedStateBootstrapFunction
import org.apache.flink.state.api.{OperatorTransformation, Savepoint}
import org.apache.flink.streaming.api.scala._

object BootstrapJobKryo {

  class AccountBootstrapper extends KeyedStateBootstrapFunction[Account, BootstrapAccount] {
    var state: ValueState[Account] = _

    @throws[Exception]
    override def open(parameters: Configuration): Unit = {
      val descriptor = new ValueStateDescriptor[Account]("previous", classOf[Account])
      state = getRuntimeContext.getState(descriptor)
    }

    override def processElement(value: BootstrapAccount, ctx: KeyedStateBootstrapFunction[Account, BootstrapAccount]#Context): Unit = {
      state.update(Account(value.id, value.amount, value.timestamp))
    }
  }

  def main(args: Array[String]) {
    val bEnv = ExecutionEnvironment.getExecutionEnvironment
    bEnv.setParallelism(1)
    bEnv.getConfig.enableForceKryo()
    bEnv.getConfig.disableAutoTypeRegistration()
    bEnv.getConfig.registerKryoType(classOf[Account])

    val accountDataSet = bEnv.fromElements(
      BootstrapAccount(1, 1.0, 1L),
      BootstrapAccount(2, 1.0, 2L),
      BootstrapAccount(3, 1.0, 3L),
      BootstrapAccount(1, 2.0, 10L),
    )

    val transformation = OperatorTransformation
      .bootstrapWith(accountDataSet)
      .keyBy((acc: BootstrapAccount) => Account(acc.id, acc.amount, acc.timestamp))
      .transform(new AccountBootstrapper)

    val maxParallelism = 128

    Savepoint
      .create(new MemoryStateBackend(), maxParallelism)
      .withOperator("uid1", transformation)
      .write("file:///tmp/flink-test/state-proc/savepointkryo1")

    bEnv.execute("Bootstrap job")
  }
}
