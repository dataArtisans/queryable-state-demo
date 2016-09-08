package com.dataartisans

import com.jgrier.flinkstuff.sources.TimestampSource
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._

import scala.util.Random

object QueryableJob {
  def main(args: Array[String]) {
    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val stream = env.addSource(new TimestampSource(100, 1))

    val fruits = Seq("apple", "orange", "pear")
    stream
      .flatMap(dataPoint => fruits.map(dataPoint.withKey(_)))
      .map(_.withNewValue(nextGaussian()))
      .keyBy(_.getKey)
      .flatMap(new QueryableWindowFunction)
      .print

    // execute program
    env.execute("queryable-job")
  }

  private def nextGaussian(): java.lang.Long = {
    Math.round(Random.nextGaussian() * 100)
  }
}

