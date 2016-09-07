package com.dataartisans

import java.{lang, util}

import com.jgrier.flinkstuff.data.KeyedDataPoint
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.common.{ExecutionConfig, JobID}
import org.apache.flink.configuration.GlobalConfiguration
import org.apache.flink.runtime.query.QueryableStateClient
import org.apache.flink.runtime.query.netty.message.KvStateRequestSerializer
import org.apache.flink.runtime.state.{VoidNamespace, VoidNamespaceSerializer}

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Await, duration}

object QueryClient {
  def main(args: Array[String]): Unit = {

    val execConfig = new ExecutionConfig
    val client: QueryableStateClient = new QueryableStateClient(GlobalConfiguration.loadConfiguration("/Users/jgrier/workspace/flink/flink-dist/src/main/resources"))

    val keySerializer: TypeSerializer[String] = TypeInformation.of(new TypeHint[String]() {}).createSerializer(null)
    val valueSerializer = TypeInformation.of(new TypeHint[KeyedDataPoint[java.lang.Long]]() {}).createSerializer(execConfig)

    val key = "apple"

    val serializedKey: Array[Byte] =
      KvStateRequestSerializer.serializeKeyAndNamespace(
        key,
        keySerializer,
        VoidNamespace.INSTANCE,
        VoidNamespaceSerializer.INSTANCE)

    val jobId = JobID.fromHexString("d3a00a857cc7e53311883f1729bf5781")

    while(true) {
      Thread.sleep(1000)
      println("-------------------------------------------")
      val future = client.getKvState(jobId, "time-series", key.hashCode, serializedKey)

      // Query for state
      val serializedResult: Array[Byte] = Await.result(future, new FiniteDuration(100, duration.SECONDS))
      val listResult: util.List[KeyedDataPoint[lang.Long]] = KvStateRequestSerializer.deserializeList(serializedResult, valueSerializer)

      val itor = listResult.iterator
      while (itor.hasNext) {
        println(itor.next)
      }
    }
  }
}
