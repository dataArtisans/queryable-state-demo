package com.twitter.hello

import java.util.concurrent.TimeUnit

import com.twitter.finagle.http.Request
import com.twitter.finatra.http.Controller
import org.joda.time.DateTime

import scala.util.Random
import scala.util.matching.Regex

class HelloWorldController extends Controller {

  get("/") { request: Request =>

  }

  get("/hi") { request: Request =>
    "Hello " + request.params.getOrElse("name", "unnamed")
  }

  post("/search") { request: Request =>
    response.ok(
      Seq("apple", "orange", "pear"))
  }

  post("/query") { query: Query =>

    def parseToMillis(interval: String): Long = {
      val unitMap = Map("ms" -> "MILLISECONDS", "s" -> "SECONDS")
      val intervalPattern = new Regex("""(\d*)(.*)""")
      val intervalPattern(period, unit) = interval
      val timeUnit = TimeUnit.valueOf(unitMap(unit))
      timeUnit.toMillis(period.toLong)
    }

    def generateNormallyDistributedDatapoints(startTime: Long, numPoints: Long, interval: Long): Array[Array[Long]] = {
      (for (i <- 0L to numPoints) yield Array(Math.round(Random.nextGaussian() * 100), startTime + i * interval)).toArray
    }

    val startTimeMs = new DateTime(query.range.from).getMillis
    val intervalMs = parseToMillis(query.interval)

    response.ok(
      query.targets.toArray.map {
        target => {
          QueryResponse(
            target = query.targets(0).target,
            datapoints = generateNormallyDistributedDatapoints(startTimeMs, query.maxDataPoints.toLong, intervalMs)
          )
        }
      }
    )
  }

  post("/hi") { hiRequest: HiRequest =>
    "Hello " + hiRequest.name + " with id " + hiRequest.id
  }
}
