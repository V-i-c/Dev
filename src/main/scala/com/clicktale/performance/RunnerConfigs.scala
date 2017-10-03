package com.clicktale.performance

import com.typesafe.config.ConfigFactory

case class RunnerConfigs(async:Boolean,numOfBins:Long)
object RunnerConfigs {
  private val runnerConfs = ConfigFactory.load().getConfig("aerospike_performance.runner")
  def apply() = new RunnerConfigs(async = runnerConfs.getBoolean("async"),numOfBins = runnerConfs.getLong("numofbins"))
}
