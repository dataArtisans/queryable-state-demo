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

object QueryableJob {
  def main(args: Array[String]) {
    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val stream = env.addSource(new TimestampSource(100, 1))

    //    stream
    //      .map(_.withKey("apple"))
    //      .keyBy(_.getKey)
    //      .timeWindow(Time.seconds(1))
    //      .apply(0L, new QueryableFoldFunction, new QueryableWindowFunction)
    //      .print

    stream
      .map(_.withKey("apple"))
      .keyBy(_.getKey)
      .flatMap(new QueryableListFlatMapFunction)
      .print

    //    stream
    //      .map(_.withKey("apple"))
    //      .keyBy(_.getKey)
    //      .asQueryableState("time-series")


    // execute program
    env.execute("queryable-job")
  }

  class QueryableFoldFunction extends RichFoldFunction[KeyedDataPoint[java.lang.Long], Long] {
    override def fold(accumulator: Long, value: KeyedDataPoint[java.lang.Long]): Long = accumulator + 1
  }

  class QueryableWindowFunction extends RichWindowFunction[Long, Long, String, TimeWindow] {
    override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[Long]): Unit = input.headOption.getOrElse(0L)
  }

}

class QueryableFlatMapFunction extends RichFlatMapFunction[KeyedDataPoint[java.lang.Long], Array[KeyedDataPoint[java.lang.Long]]] {
  private lazy val state = getRuntimeContext.getState(stateDescriptor)
  private val stateDescriptor = new ValueStateDescriptor("time-series", TypeInformation.of(new TypeHint[mutable.LinkedList[KeyedDataPoint[lang.Long]]]() {}), new mutable.LinkedList[KeyedDataPoint[lang.Long]])
  stateDescriptor.setQueryable("time-series")

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)

  }

  override def flatMap(value: KeyedDataPoint[java.lang.Long], out: Collector[Array[KeyedDataPoint[java.lang.Long]]]): Unit = {
    val elements = state.value();
    state.update(
      if (elements.size >= 1000) {
        elements.drop(1) :+ value
      }
      else {
        elements :+ value
      })
  }
}

class QueryableListFlatMapFunction extends RichFlatMapFunction[KeyedDataPoint[java.lang.Long], Array[KeyedDataPoint[java.lang.Long]]] {
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
    if (count == 5) {
      listState.clear()
      count = 0
    }
    listState.add(value)
    countState.update(count + 1)
  }
}