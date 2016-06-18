package com.inferwerx.ogliabilitytracker

import com.inferwerx.ogliabilitytracker.verticles.ApiServer
import io.vertx.core.AbstractVerticle
import io.vertx.core.DeploymentOptions
import io.vertx.core.Future

class Main : AbstractVerticle() {
    /**
     * Starts all of the Vert.x verticles that are required for this server. Since most things in vertx.x are async,
     * futures are used in order to determine when everything has started.
     */
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