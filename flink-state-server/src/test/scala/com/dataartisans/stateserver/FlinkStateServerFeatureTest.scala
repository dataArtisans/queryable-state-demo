package com.dataartisans.stateserver

import com.dataartisans.stateserver.server.FlinkStateServer
import com.twitter.finagle.http.Status._
import com.twitter.finatra.http.test.EmbeddedHttpServer
import com.twitter.inject.server.FeatureTest

class FlinkStateServerFeatureTest extends FeatureTest {

  override val server = new EmbeddedHttpServer(new FlinkStateServer)

  "Server" should {
    "Say hi" in {
      server.httpGet(
        path = "/hi?name=Bob",
        andExpect = Ok,
        withBody = "Hello Bob")
    }
    "Say hi for Post" in {
      server.httpPost(
        path = "/hi",
        postBody =
          """
          {
            "id": 10,
            "name" : "Sally"
          }
          """,
        andExpect = Ok,
        withBody = "Hello Sally with id 10")
    }
    "Execute query" in {
      server.httpPost(
        path = "/query",
        postBody =
          """
            {
              "range": { "from": "2015-12-22T03:06:13.851Z", "to": "2015-12-22T06:48:24.137Z" },
              "interval": "5s",
              "targets": [
                { "refId": "B", "target": "upper_75" },
                { "refId": "A", "target": "upper_90" }
              ],
              "format": "json",
              "maxDataPoints": 2495
            }
          """,
        andExpect = Ok,
        withJsonBody =
          """
            [
              {
                "target":"upper_75",
                "datapoints":[
                  [622,1450754160000],
                  [365,1450754220000]
                ]
              },
              {
                "target":"upper_90",
                "datapoints":[
                  [861,1450754160000],
                  [767,1450754220000]
                ]
              }
            ]
          """
      )
    }
  }
}
