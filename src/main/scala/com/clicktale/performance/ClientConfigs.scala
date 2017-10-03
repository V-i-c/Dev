package com.clicktale.performance

import com.typesafe.config.ConfigFactory
import scala.collection.JavaConverters._

case class ClientConfigs(port:Int, hosts:Set[String], namespace:String, timeout:Int, setName:String, binName:String)
object ClientConfigs {
  private val conn = ConfigFactory.load()

  def apply() = {
    val aerospikeSection = conn.getConfig("aerospike_performance.aerospike")
    new ClientConfigs(port = aerospikeSection.getInt("port"),hosts = aerospikeSection.getStringList("nodes").asScala.toSet,
      namespace = aerospikeSection.getString("namespace"),timeout = aerospikeSection.getInt("timeoutmsec"),setName =
        aerospikeSection.getString("setname"),binName = aerospikeSection.getString("binname"))
  }

}
