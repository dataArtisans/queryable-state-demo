package com.dataartisans.stateserver.server

import java.util.concurrent.TimeUnit
import javax.inject.Inject

import com.dataartisans.stateserver.data.{Query, QueryResponse}
import com.dataartisans.stateserver.queryclient.QueryClient
import com.twitter.finagle.http.Request
import com.twitter.finatra.http.Controller
import org.joda.time.DateTime

import scala.collection.JavaConverters._
import scala.util.matching.Regex

class FlinkStateServerController @Inject()(queryClient: QueryClient) extends Controller {

  post("/search") { request: Request =>
    response.ok(
      Seq("apple", "orange", "pear"))
  }

  post("/query") { query: Query =>

    val startTimeMs = new DateTime(query.range.from).getMillis
    val intervalMs = parseToMillis(query.interval)

    response.ok(
      query.targets.toArray.map {
        queryTarget => {
          QueryResponse(
            target = queryTarget.target,
            datapoints = queryFlink(queryTarget.target, startTimeMs, query.maxDataPoints.toLong, intervalMs)
          )
        }
      }
    )
  }

  def queryFlink(key: String, startTime: Long, numPoints: Long, interval: Long): Array[Array[Long]] = {
    val results = queryClient.executeQuery(key).asScala.toArray
    results.map { dataPoint =>
      Array(dataPoint.getValue.toLong, dataPoint.getTimeStampMs)
    }
  }

  def parseToMillis(interval: String): Long = {
    val unitMap = Map("ms" -> "MILLISECONDS", "s" -> "SECONDS")
    val intervalPattern = new Regex("""(\d*)(.*)""")
    val intervalPattern(period, unit) = interval
    val timeUnit = TimeUnit.valueOf(unitMap(unit))
    timeUnit.toMillis(period.toLong)
  }

}
