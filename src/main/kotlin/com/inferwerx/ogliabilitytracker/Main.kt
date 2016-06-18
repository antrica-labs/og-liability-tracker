package com.inferwerx.ogliabilitytracker

import com.inferwerx.ogliabilitytracker.exceptions.MultiAsyncException
import com.inferwerx.ogliabilitytracker.verticles.AlbertaLiabilityImporter
import com.inferwerx.ogliabilitytracker.verticles.ApiServer
import com.inferwerx.ogliabilitytracker.verticles.DatabaseScriptRunner
import io.vertx.core.AbstractVerticle
import io.vertx.core.CompositeFuture
import io.vertx.core.DeploymentOptions
import io.vertx.core.Future
import java.util.*

/**
 * This is the entry point to the server application. In order to run it, you need to execute the vert.x Launcher class
 * with the correct options. For example:
 *
 * java io.vertx.core.Launcher run com.inferwerx.ogliabilitytracker.Main -conf src/main/conf/server-config.json
 */
class Main : AbstractVerticle() {
    /**
     * Starts all of the Vert.x verticles that are required for this server. Since most things in vertx.x are async,
     * futures are used in order to determine when everything has started.
     */
    override fun start(startFuture : Future<Void>) {
        val dbScriptRunnerFuture = Future.future<Void>() // Used to determine if the db script runner verticle started
        val importStartFuture = Future.future<Void>()    // Used to determine if the liability importer verticles started
        val importDeploymentErrors = LinkedList<Throwable>()

        val apiDeploymentOptions = DeploymentOptions().setConfig(config())
        val workerDeploymentOptions = DeploymentOptions().setWorker(true).setConfig(config())

        // Deploy a single DatabaseScriptRunner worker verticle
        vertx.deployVerticle(DatabaseScriptRunner(), workerDeploymentOptions) {
            if (it.failed())
                dbScriptRunnerFuture.fail(it.cause())
            else
                dbScriptRunnerFuture.complete()
        }

        // Create as many importer verticles as there are CPU cores for faster processing
        for (i in 1..Runtime.getRuntime().availableProcessors()) {
            vertx.deployVerticle(AlbertaLiabilityImporter(), workerDeploymentOptions) {
                if (it.failed())
                    importDeploymentErrors.add(it.cause())

                if (i == Runtime.getRuntime().availableProcessors()) {
                    if (importDeploymentErrors.count() > 0)
                        importStartFuture.fail(MultiAsyncException(importDeploymentErrors))
                    else
                        importStartFuture.complete()
                }
            }
        }

        // The API server should only start if the worker verticles started
        CompositeFuture.all(dbScriptRunnerFuture, importStartFuture).setHandler {
            if (it.failed()) {
                startFuture.fail(it.cause())

                System.exit(0)

                return@setHandler
            }

            vertx.deployVerticle(ApiServer(), apiDeploymentOptions) {
                if (it.succeeded()) {
                    startFuture.complete()
                } else {
                    startFuture.fail(it.cause())
                }
            }
        }
    }
}