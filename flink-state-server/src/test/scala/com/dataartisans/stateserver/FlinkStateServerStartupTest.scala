package com.dataartisans.stateserver

import com.dataartisans.stateserver.server.FlinkStateServer
import com.google.inject.Stage
import com.twitter.finatra.http.test.EmbeddedHttpServer
import com.twitter.inject.server.FeatureTest

class FlinkStateServerStartupTest extends FeatureTest {

  override val server = new EmbeddedHttpServer(
    twitterServer = new FlinkStateServer,
    stage = Stage.PRODUCTION,
    verbose = false)

  "Server" should {
    "startup" in {
      server.assertHealthy()
    }
  }
}
