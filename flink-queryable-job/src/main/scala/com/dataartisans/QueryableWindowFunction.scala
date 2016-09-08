package com.dataartisans

import com.jgrier.flinkstuff.data.KeyedDataPoint
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ListStateDescriptor, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector

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
