package com.clicktale.performance

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Random

object Runner extends App with CompareOps{
  val runConfigs = RunnerConfigs()
  val repo = AerospikeRepo[Array[Byte],Array[Byte]]()
  implicit val ex = ExecutionContext.global
  val kb = 1024
  val x = Array.fill(10 * kb)(0.toByte)
  val y = Array.fill(15 * kb)(0.toByte)
  val z = Array.fill(20 * kb)(0.toByte)

  def content = {
    new Random().nextInt(2) match {
      case 0 => x
      case 1 => y
      case 2 => z
    }
  }

  timeWrite(content)
  println("---------------------------")
  timeRead()
  repo.close()

  def timeWrite(content: => Array[Byte]) = {
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
         |All sync writes of ${runConfigs.numOfBins} bins finished in: ${batchInsertTime/1000000000d} seconds
         |Minimum sync write of ${runConfigs.numOfBins} bins finished in: ${minInsertTime/1000000d} millis
         |Maximum sync write of ${runConfigs.numOfBins} bins finished in: ${maxInsertTime/1000000d} millis
         |Median sync write of ${runConfigs.numOfBins} bins finished in: ${medianInsertTime/1000000d} millis
         |Average sync write of ${runConfigs.numOfBins} bins finished in: ${averageInsertTime/1000000d} millis
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
         |All sync reads of ${runConfigs.numOfBins} bins finished in: ${batchReadTime/1000000000d} seconds
         |Minimum sync read of ${runConfigs.numOfBins} bins finished in: ${minReadTime/1000000d} millis
         |Maximum sync read of ${runConfigs.numOfBins} bins finished in: ${maxReadTime/1000000d} millis
         |Median sync read of ${runConfigs.numOfBins} bins finished in: ${medianReadTime/1000000d} millis
         |Average sync read of ${runConfigs.numOfBins} bins finished in: ${averageReadTime/1000000d} millis
       """.stripMargin)
  }

  def asyncTimeWrite(content: => Array[Byte]) = {
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
      s"""All async writes of ${runConfigs.numOfBins} bins finished in: ${bt/1000000000d} seconds
         |Minimum async write of ${runConfigs.numOfBins} bins finished in: ${mint/1000000d} millis
         |Maximum async write of ${runConfigs.numOfBins} bins finished in: ${maxt/1000000d} millis
         |Median async write of ${runConfigs.numOfBins} bins finished in: ${med/1000000d} millis
         |Average async write of ${runConfigs.numOfBins} bins finished in: ${avg/1000000d} millis
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
      s"""All async reads of ${runConfigs.numOfBins} bins finished in: ${bt/1000000000d} seconds
         |Minimum async of ${runConfigs.numOfBins} bins reads finished in: ${mint/1000000d} millis
         |Maximum async of ${runConfigs.numOfBins} bins reads finished in: ${maxt/1000000d} millis
         |Media async of ${runConfigs.numOfBins} bins reads finished in: ${med/1000000d} millis
         |Average async of ${runConfigs.numOfBins} bins reads finished in: ${avg/1000000d} millis
       """.stripMargin)
  }
}
