package com.guanshi.cloudhotel.mock

import org.apache.flume.Event
import org.apache.flume.api.RpcClientFactory

class FlumeClient(val hostname: String, val port: Int) {
  val client = RpcClientFactory.getDefaultInstance(hostname, port)

  def sendEvent(event: Event): Unit = {
    client.append(event)
  }

  def cleanUp(): Unit = {
    client.close()
  }
}

object FlumeClient {
  def apply(hostname: String, port: Int) = new FlumeClient(hostname, port)
}
