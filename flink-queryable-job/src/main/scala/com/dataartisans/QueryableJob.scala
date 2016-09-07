package com.dataartisans

import java.lang

import com.jgrier.flinkstuff.data.KeyedDataPoint
import com.jgrier.flinkstuff.sources.TimestampSource
import org.apache.flink.api.common.functions.{RichFlatMapFunction, RichFoldFunction, RichReduceFunction}
import org.apache.flink.api.common.state.{ListStateDescriptor, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.{TypeInformation, TypeHint}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.RichWindowFunction
import org.apache.flink.streaming.api.windowing.windows.{TimeWindow, Window}
import org.apache.flink.util.Collector

import scala.collection.mutable
import scala.util.Random

object QueryableJob {
  def main(args: Array[String]) {
    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val stream = env.addSource(new TimestampSource(100, 1))

    //(for (i <- 0L to numPoints) yield Array(Math.round(Random.nextGaussian() * 100), startTime + i * interval)).toArray

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

class QueryableWindowFunction extends RichFlatMapFunction[KeyedDataPoint[java.lang.Long], Array[KeyedDataPoint[java.lang.Long]]] {
  private lazy val countState = getRuntimeContext.getState(countDesc)
  private val countDesc = new ValueStateDescriptor("time-series-count", TypeInformation.of(new TypeHint[Int]() {}), 0)

  private lazy val listState = getRuntimeContext.getListState(listStateDescriptor)
  private val listStateDescriptor = new ListStateDescriptor("time-series", TypeInformation.of(new TypeHint[KeyedDataPoint[java.lang.Long]]() {}))
  listStateDescriptor.setQueryable("time-series")

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
  }

  override def flatMap(value: KeyedDataPoint[java.lang.Long], out: Collector[Array[KeyedDataPoint[java.lang.Long]]]): Unit = {
    var count = countState.value
    val maxWindowSize: Int = 200
    if (count == maxWindowSize) {
      listState.clear()
      count = 0
    }
    listState.add(value)
    countState.update(count + 1)
  }
}