package com.twitter.hello

import java.{lang, util}

import com.jgrier.flinkstuff.data.KeyedDataPoint
import org.apache.flink.api.common.{JobID, ExecutionConfig}
import org.apache.flink.api.common.typeinfo.{TypeInformation, TypeHint}
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.configuration.GlobalConfiguration
import org.apache.flink.runtime.query.QueryableStateClient
import org.apache.flink.runtime.query.netty.message.KvStateRequestSerializer
import org.apache.flink.runtime.state.{VoidNamespaceSerializer, VoidNamespace}

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Await, duration}

object QueryClient {
  def main(args: Array[String]): Unit = {
    val queryClient = QueryClient()
    while (true) {
      Thread.sleep(1000)
      val results = queryClient.executeQuery
      val itor = results.iterator
      while (itor.hasNext) {
        println(itor.next)
      }
    }
  }

  def apply() = {
    new QueryClient
  }
}

class QueryClient {
  private val client = getQueryableStateClient()

  def executeQuery: util.List[KeyedDataPoint[lang.Long]] = {
    val jobId = JobID.fromHexString("d3a00a857cc7e53311883f1729bf5781")
    val key = "apple"

    // Serialize request
    val seralizedKey = getSeralizedKey("apple")

    // Query Flink state
    val future = client.getKvState(jobId, "time-series", key.hashCode, seralizedKey)

    // Await async result
    val serializedResult: Array[Byte] = Await.result(future, new FiniteDuration(10, duration.SECONDS))

    // Deserialize response
    val results = deserializeResponse(serializedResult)

    results
  }

  private def deserializeResponse(serializedResult: Array[Byte]): util.List[KeyedDataPoint[java.lang.Long]] = {
    KvStateRequestSerializer.deserializeList(serializedResult, getValueSerializer())
  }

  private def getQueryableStateClient(): QueryableStateClient = {
    val execConfig = new ExecutionConfig
    val client: QueryableStateClient = new QueryableStateClient(GlobalConfiguration.loadConfiguration("/Users/jgrier/workspace/flink/flink-dist/src/main/resources"))
    client
  }

  private def getValueSerializer(): TypeSerializer[KeyedDataPoint[java.lang.Long]] = {
    TypeInformation.of(new TypeHint[KeyedDataPoint[lang.Long]]() {}).createSerializer(new ExecutionConfig)
  }

  private def getSeralizedKey(key: String): Array[Byte] = {
    val keySerializer: TypeSerializer[String] = TypeInformation.of(new TypeHint[String]() {}).createSerializer(null)
    val serializedKey: Array[Byte] =
      KvStateRequestSerializer.serializeKeyAndNamespace(
        key,
        keySerializer,
        VoidNamespace.INSTANCE,
        VoidNamespaceSerializer.INSTANCE)
    serializedKey
  }
}