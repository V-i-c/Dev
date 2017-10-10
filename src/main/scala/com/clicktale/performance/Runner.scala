package com.clicktale.performance

import scala.concurrent.{ExecutionContext, Future}

object Runner extends App with CompareOps{
  val runConfigs = RunnerConfigs()
  val repo = AerospikeRepo[Array[Byte],Array[Byte]]()
  implicit val ex = ExecutionContext.global
  val content = Array.fill(1024)(0.toByte)

  timeWrite(content)
  println("---------------------------")
  timeRead()
  repo.close()

  def timeWrite(content:Array[Byte]) = {
    val allIds = 0l until runConfigs.numOfBins
    val beforeTime = System.nanoTime()

    val writeTimes = allIds.map{ id =>
      val before = System.nanoTime()
      repo.write(id,content)
      val after = System.nanoTime()
      before -> after
    }

    def batchInsertTime() = {
      implicit val gt = max
      val maxWriteTime = writeTimes.collect{case(_,a) => a}.reduceLeft(pick)
      maxWriteTime - beforeTime
    }

    def singleInsertTime() = {
      writeTimes.map{case (b,a) => a-b}
    }

    def minInsertTime() = {
      implicit val lt = min
      singleInsertTime().reduceLeft(pick)
    }
    def maxInsertTime() = {
      implicit val gt = max
      singleInsertTime().reduceLeft(pick)
    }
    def medianInsertTime() = {
      val sorted = singleInsertTime().sorted
      sorted(runConfigs.numOfBins/2)
    }
    def averageInsertTime() = {
      (singleInsertTime().reduceLeft(_ + _))/runConfigs.numOfBins
    }
    println(
      s"""
         |All sync writes of ${runConfigs.numOfBins} bins finished in: $batchInsertTime nano
         |Minimum sync write of ${runConfigs.numOfBins} bins finished in: $minInsertTime nano
         |Maximum sync write of ${runConfigs.numOfBins} bins finished in: $maxInsertTime nano
         |Median sync write of ${runConfigs.numOfBins} bins finished in: $medianInsertTime nano
         |Average sync write of ${runConfigs.numOfBins} bins finished in: $averageInsertTime nano
       """.stripMargin)
  }

  def timeRead() = {
    val allIds = 0l until runConfigs.numOfBins
    val beforeTime = System.nanoTime()

    val readTimes = allIds.map{ id =>
      val before = System.nanoTime()
      repo.read(id)
      val after = System.nanoTime()
      before -> after
    }

    def singleReadTime = {
      readTimes.map{case (b,a) => a-b}
    }

    def batchReadTime = {
      implicit val gt = max
      val maxTime = readTimes.collect {case(_,a) => a}.reduceLeft(pick)
      maxTime - beforeTime
    }

    def minReadTime = {
      implicit val lt = min
      singleReadTime.reduceLeft(pick)
    }
    def maxReadTime = {
      implicit val gt = max
      singleReadTime.reduceLeft(pick)
    }
    def averageReadTime = singleReadTime.reduceLeft(_+_) / runConfigs.numOfBins
    def medianReadTime() = {
      val sorted = singleReadTime.sorted
      sorted(runConfigs.numOfBins/2)
    }
    println(
      s"""
         |All sync reads of ${runConfigs.numOfBins} bins finished in: $batchReadTime nano
         |Minimum sync read of ${runConfigs.numOfBins} bins finished in: $minReadTime nano
         |Maximum sync read of ${runConfigs.numOfBins} bins finished in: $maxReadTime nano
         |Median sync read of ${runConfigs.numOfBins} bins finished in: $medianReadTime nano
         |Average sync read of ${runConfigs.numOfBins} bins finished in: $averageReadTime nano
       """.stripMargin)
  }

  def asyncTimeWrite(content:Array[Byte]) = {
    def timedWrite(id: Long) = {
      val before = System.nanoTime()
      val k = repo.writeAsync(id, content)
      k.map(_ => before -> System.nanoTime())
    }
    val groupOfBins = 0l until runConfigs.numOfBins
    val beforeWritesStarted = System.nanoTime()
    val writeTimes = Future.sequence(groupOfBins.map(timedWrite))

    def batchWriteNanos() = {

      val maxTimeFinished = {
        implicit val gt = max
        for {
          seq <- writeTimes
          maxTime = seq.collect{case (_,a) => a}.reduceLeft(pick)
        }yield maxTime
      }

      for {
       t2 <- maxTimeFinished
      } yield t2 - beforeWritesStarted
    }


    def singleBinWriteTime(implicit f:(Long,Long) => Boolean) = {
      for {
       seq <- writeTimes
       insertionsTime = seq.collect{case (b,a) => a - b}.reduceLeft(pick)
      } yield insertionsTime
    }

    def medinanBinWriteTime() = {
      for {
        seq <- writeTimes
        insertionsTime = seq.collect{case (b,a) => a - b}.sorted
      }yield insertionsTime(runConfigs.numOfBins/2)
    }

    def averageBinWrite() = {
      for {
        seq <- writeTimes
        avg = seq.collect{case (b,a) => a - b}.reduceLeft(_+_)/runConfigs.numOfBins
      }yield avg
    }

    def maxBinWriteTime() = {
      singleBinWriteTime(max)
    }

    def minBinWriteTime() = {
      singleBinWriteTime(min)
    }

    for {
      bt <- batchWriteNanos()
      mint <- minBinWriteTime()
      maxt <- maxBinWriteTime()
      avg <- averageBinWrite()
      med <- medinanBinWriteTime()
    } println(
      s"""All async writes of ${runConfigs.numOfBins} bins finished in: $bt nanos
         |Minimum async write of ${runConfigs.numOfBins} bins finished in: $mint nanos
         |Maximum async write of ${runConfigs.numOfBins} bins finished in: $maxt nanos
         |Median async write of ${runConfigs.numOfBins} bins finished in: $med nanos
         |Average async write of ${runConfigs.numOfBins} bins finished in: $avg nanos
       """.stripMargin)
  }

  def asyncTimeRead() = {
    def timedRead(id:Long) = {
      val before = System.nanoTime()
      val k = repo.readAsync(id)
      k.map(_ => before -> System.nanoTime())
    }

    val timeBeforeAllReads = System.nanoTime()
    val readTimes = Future.sequence((0l until runConfigs.numOfBins).map(timedRead))


    def batchReadNanos() = {
      implicit val gt = max
      for {
        seq <- readTimes
        maxTime = seq.collect{case (_,a) => a}.reduceLeft(pick)

      } yield maxTime - timeBeforeAllReads
    }


    def singleBinReadTime(implicit f:(Long,Long) => Boolean) = {
      for {
        seq <- readTimes
        t = seq.collect{case (b,a) => a-b}.reduceLeft(pick)
      } yield t
    }

    def averageBinRead() = for {
      seq <- readTimes
      avg = seq.collect{case (b,a) => a-b}.reduceLeft(_+_)/runConfigs.numOfBins
    } yield avg


    def medianBinReadTime() = for {
      seq <- readTimes
      sorted = seq.collect{case (b,a) => a-b}.sorted
    }yield sorted(runConfigs.numOfBins/2)

    def maxBinReadTime () = singleBinReadTime(max)


    def minBinReadTime() = singleBinReadTime(min)

    for {
      bt <- batchReadNanos()
      mint <- minBinReadTime()
      maxt <- maxBinReadTime()
      avg <- averageBinRead()
      med <- medianBinReadTime()
    } println(
      s"""All async reads of ${runConfigs.numOfBins} bins finished in: $bt nanos
         |Minimum async of ${runConfigs.numOfBins} bins reads finished in: $mint nanos
         |Maximum async of ${runConfigs.numOfBins} bins reads finished in: $maxt nanos
         |Media async of ${runConfigs.numOfBins} bins reads finished in: $med nanos
         |Average async of ${runConfigs.numOfBins} bins reads finished in: $avg nanos
       """.stripMargin)
  }
}
