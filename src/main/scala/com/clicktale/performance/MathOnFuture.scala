package com.clicktale.performance

trait MathOnFuture {
  val max = (x:Long,y:Long) => x > y
  val min = (x:Long,y:Long) => x < y

  def pick(t1:Long, t2:Long)(implicit f:(Long,Long) => Boolean) = {
    if(f(t1,t2)) t1 else t2
  }

}
