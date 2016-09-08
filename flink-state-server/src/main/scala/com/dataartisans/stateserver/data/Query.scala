package com.dataartisans.stateserver.data

import com.fasterxml.jackson.annotation.JsonProperty

// Some(
//   Map(
//     format -> json,
//     targets -> List(Map(target -> piper, refId -> A)),
//     panelId -> 1.0,
//     interval -> 10s,
//     range -> Map(from -> 2015-12-22T03:00:00.000Z, to -> 2015-12-22T07:00:00.000Z),
//     maxDataPoints -> 1440.0,
//     rangeRaw -> Map(from -> 2015-12-22T03:00:00.000Z, to -> 2015-12-22T07:00:00.000Z)))
case class QueryRange(from: String, to: String)

case class QueryTarget(@JsonProperty("refId") refId: String, target: String)

case class Query(
                  @JsonProperty("panelId") panelId: String,
                  range: QueryRange,
                  @JsonProperty("rangeRaw") rangeRaw: QueryRange,
                  interval: String,
                  targets: List[QueryTarget],
                  format: String,
                  @JsonProperty("maxDataPoints") maxDataPoints: Double)
