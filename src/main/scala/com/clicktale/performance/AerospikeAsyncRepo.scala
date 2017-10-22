package com.clicktale.performance

import com.aerospike.client._
import com.aerospike.client.async.{EventPolicy, NettyEventLoops}
import com.aerospike.client.listener.{RecordListener, WriteListener}
import com.aerospike.client.policy.{ClientPolicy, WritePolicy}
import io.netty.channel.epoll.EpollEventLoopGroup

import scala.collection.JavaConverters._

import scala.concurrent.{ExecutionContext, Promise}

class AerospikeAsyncRepo[+A, -B] private (private val client: AerospikeClient, conf: ClientConfigs) {

  import AerospikeAsyncRepo._

  private val defaultWrPolicy = {
    val pol = new WritePolicy
    pol.maxRetries  = 10
    pol.setTimeout(conf.timeout)
    val noExpiration = -1
    pol.expiration = noExpiration
    pol
  }

  def close() = client.close()

  def writeAsync(sid:Long, content:B)(implicit ex:ExecutionContext) = {
    val bin = new Bin(conf.binName, content)
    val key = new Key(conf.namespace, conf.setName, sid)
    val p = Promise[Key]
    val listener = new WriteListener {
      override def onFailure(exception: AerospikeException): Unit = p.failure(exception)

      override def onSuccess(key: Key): Unit = p.success(key)
    }

    val loop = EventLoops.next()
    client.put(loop, listener, defaultWrPolicy, key, bin)
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
    val loop = EventLoops.next()
    client.get(loop, new RecordListener {
      override def onFailure(exception: AerospikeException): Unit = p.failure(exception)

      override def onSuccess(key: Key, record: Record): Unit = p.success(key ->
        record.bins.asScala(conf.binName).asInstanceOf[A])
    }, client.getReadPolicyDefault, key, conf.binName)
    p.future
  }
}

object AerospikeAsyncRepo {
  private val conf = ClientConfigs()

  val EventLoops = createLoops()

  private def createLoops() = {
    val eventPolicy = new EventPolicy()
    eventPolicy.commandsPerEventLoop = 100
    val epollGroup = new EpollEventLoopGroup(Runtime.getRuntime.availableProcessors())
    new NettyEventLoops(eventPolicy, epollGroup)
  }

  private def asyncClient() = {
    val hosts = conf.hosts.map(i => new Host(i, conf.port)).toArray

    val clientPolicy = new ClientPolicy()
    clientPolicy.eventLoops = EventLoops
    val timeout = 10000
    clientPolicy.timeout = timeout
    clientPolicy.readPolicyDefault.setTimeout(timeout)
    clientPolicy.writePolicyDefault.setTimeout(timeout)

    clientPolicy.readPolicyDefault.maxRetries = 0
    clientPolicy.writePolicyDefault.maxRetries = 0
    clientPolicy.maxConnsPerNode = 100
    clientPolicy.batchPolicyDefault.setTimeout(timeout * 100)

    new AerospikeClient(clientPolicy, hosts: _*)
  }

  def apply[A,B]() = new AerospikeAsyncRepo[A, B](asyncClient(), conf)
}

