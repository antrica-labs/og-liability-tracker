package com.inferwerx.ogliabilitytracker

import com.inferwerx.ogliabilitytracker.verticles.ApiServer
import io.vertx.core.AbstractVerticle
import io.vertx.core.DeploymentOptions
import io.vertx.core.Future

class Main : AbstractVerticle() {
    override fun start(startFuture : Future<Void>) {
        val apiDeploymentOptions = DeploymentOptions().setConfig(config())

        vertx.deployVerticle(ApiServer(), apiDeploymentOptions) {
            if (it.succeeded()) {
                startFuture.complete()
            } else {
                startFuture.fail(it.cause())
            }
        }
    }
}