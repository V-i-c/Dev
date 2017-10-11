package com.clicktale.performance

import java.util.concurrent.ThreadFactory

trait DeamonTheadFactory {

  val deamonFactory = new ThreadFactory() {

    override def newThread(r:Runnable) = {
      val thread = new Thread(r)
      thread.setDaemon(true)
      thread
    }
  }
}
