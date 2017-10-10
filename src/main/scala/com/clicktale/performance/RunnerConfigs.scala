package com.clicktale.performance

import com.typesafe.config.ConfigFactory

case class RunnerConfigs(numOfBins:Int)
object RunnerConfigs {
  private val runnerConfs = ConfigFactory.load().getConfig("aerospike_performance.runner")
  def apply() = new RunnerConfigs(numOfBins = runnerConfs.getInt("numofbins"))
}
