package com.clicktale.performance

import com.aerospike.client._
import com.aerospike.client.async.{AsyncClient, AsyncClientPolicy}
import com.aerospike.client.listener.{RecordListener, WriteListener}
import com.aerospike.client.policy.WritePolicy

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Promise}

class AerospikeRepo[+A,-B] private (private val client:AsyncClient,conf:ClientConfigs) {

  private val defaultWrPolicy = {
    val pol = new WritePolicy
    pol.maxRetries  = 0
    pol.setTimeout(conf.timeout)
    val noExpiration = 1
    pol.expiration = noExpiration
    pol
  }

  def writeAsync(sid:Long, content:B)(implicit ex:ExecutionContext) = {
    val bin = new Bin(conf.binName, content)
    val key = new Key(conf.namespace, conf.setName, sid)
    val p = Promise[Key]
    val listener = new WriteListener {
      override def onFailure(exception: AerospikeException): Unit = p.failure(exception)

      override def onSuccess(key: Key): Unit = p.success(key)
    }
    client.put(defaultWrPolicy,listener,key,bin)
    p.future
  }

  def write(sid: Long, content: B) = {
    val bin = new Bin(conf.binName, content)
    val key = new Key(conf.namespace, conf.setName, sid)
    client.put(defaultWrPolicy, key, bin)
  }

  def read(sid:Long) = {
    val key = new Key(conf.namespace, conf.setName, sid)
    val record = client.get(client.readPolicyDefault, key)
    val data = record.getValue(conf.binName)
    data.asInstanceOf[A]
  }

  def readAsync(sid:Long)(implicit ex:ExecutionContext) = {
    val key = new Key(conf.namespace, conf.setName, sid)
    val p = Promise[(Key,A)]
    client.get(client.getReadPolicyDefault,new RecordListener {
      override def onFailure(exception: AerospikeException): Unit = p.failure(exception)

      override def onSuccess(key: Key, record: Record): Unit = p.success(key ->
        record.bins.asScala(conf.binName).asInstanceOf[A])
    },key,conf.binName)
    p.future
  }
}

object AerospikeRepo {
  private val conf = ClientConfigs()

  private def client() = {
    val asyncPolicy = new AsyncClientPolicy
    val tout = 1000
    asyncPolicy.timeout = tout
    asyncPolicy.readPolicyDefault.setTimeout(tout)
    asyncPolicy.writePolicyDefault.setTimeout(tout)

    asyncPolicy.readPolicyDefault.maxRetries = 0
    asyncPolicy.writePolicyDefault.maxRetries = 0
    asyncPolicy.asyncMaxCommands = 100
    asyncPolicy.batchPolicyDefault.setTimeout(tout * 100)

    val hosts = conf.hosts.map(i => new Host(i, conf.port)).toArray
    new AsyncClient(asyncPolicy, hosts: _*)
  }

  def apply[A,B]() = new AerospikeRepo[A, B](client(),conf)
}
