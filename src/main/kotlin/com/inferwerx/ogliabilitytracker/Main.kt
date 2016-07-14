package com.inferwerx.ogliabilitytracker

import com.inferwerx.ogliabilitytracker.verticles.ApiServer
import com.inferwerx.ogliabilitytracker.verticles.workers.importers.AcquisitionImporter
import com.inferwerx.ogliabilitytracker.verticles.workers.importers.AlbertaLiabilityImporter
import com.inferwerx.ogliabilitytracker.verticles.workers.importers.DispositionImporter
import com.inferwerx.ogliabilitytracker.verticles.workers.importers.HierarchyImporter
import com.inferwerx.ogliabilitytracker.verticles.workers.liabilities.DetailedReportExporter
import com.inferwerx.ogliabilitytracker.verticles.workers.liabilities.HistoricalRatingsForecaster
import com.inferwerx.ogliabilitytracker.verticles.workers.liabilities.MosaicGrowthForecaster
import com.inferwerx.ogliabilitytracker.verticles.workers.liabilities.PublicDataHistory
import com.inferwerx.ogliabilitytracker.verticles.workers.util.DatabaseScriptRunner
import io.vertx.core.AbstractVerticle
import io.vertx.core.CompositeFuture
import io.vertx.core.DeploymentOptions
import io.vertx.core.Future
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.ext.jdbc.JDBCClient
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
        val dbConfig = JsonObject()
                .put("driver_class", config().getString("db.jdbc_driver"))
                .put("url", "${config().getString("db.url_proto")}${config().getString("db.file_path")}${config().getString("db.url_options")}")
                .put("user", config().getString("db.username"))
                .put("password", config().getString("db.password"))
        val dbClient = JDBCClient.createShared(vertx, dbConfig)
        val verticleFutures = LinkedList<Future<Any>>()

        val apiDeploymentOptions = DeploymentOptions().setConfig(config())
        val workerDeploymentOptions = DeploymentOptions().setWorker(true).setConfig(config())

        // Deploy all of the worker verticles
        verticleFutures.add(deployWorker(DatabaseScriptRunner(), workerDeploymentOptions))
        verticleFutures.add(deployWorker(AlbertaLiabilityImporter(), workerDeploymentOptions))
        verticleFutures.add(deployWorker(HierarchyImporter(), workerDeploymentOptions))
        verticleFutures.add(deployWorker(DispositionImporter(), workerDeploymentOptions))
        verticleFutures.add(deployWorker(HistoricalRatingsForecaster(), workerDeploymentOptions))
        verticleFutures.add(deployWorker(DetailedReportExporter(), workerDeploymentOptions))
        verticleFutures.add(deployWorker(AcquisitionImporter(), workerDeploymentOptions))
        verticleFutures.add(deployWorker(PublicDataHistory(), workerDeploymentOptions))
        verticleFutures.add(deployWorker(MosaicGrowthForecaster(), workerDeploymentOptions))

        try {
            // The API server should only start if all of the worker verticles have started
            CompositeFuture.all(verticleFutures).setHandler {
                if (it.failed())
                    throw it.cause()

                // Check and setup the database if it doesn't exist
                dbClient.getConnection {
                    if (it.failed())
                        throw it.cause()

                    val connection = it.result()


                    // Check for a valid database, then launch the API server
                    val dbCreateFuture = Future.future<Void>()

                    val dbTestQuery = "SELECT * FROM provinces"

                    connection.query(dbTestQuery) { query ->
                        if (query.failed()) {
                            // A table likely doesn't exist, indicating that the database objects haven't been created
                            val scripts = vertx.fileSystem().readDirBlocking(config().getString("db.create_script_folder"))

                            val array = JsonArray()
                            for (script in scripts) {
                                array.add(script)
                            }

                            vertx.eventBus().send<String>("og-liability-tracker.db_script_runner", JsonObject().put("scripts", array).encode()) { reply ->
                                if (reply.failed())
                                    throw reply.cause()

                                dbCreateFuture.complete()
                            }
                        } else {
                            dbCreateFuture.complete()
                        }
                    }

                    dbCreateFuture.setHandler {
                        if (it.failed())
                            throw it.cause()

                        val dbPopulatedFuture = Future.future<Void>()

                        connection.query(dbTestQuery) { query ->
                            if (query.failed())
                                throw query.cause()

                            if (query.result().numRows == 0) {
                                // The database is empty, so populate it with the basics...
                                val scripts = vertx.fileSystem().readDirBlocking(config().getString("db.populate_script_folder"))

                                val array = JsonArray()
                                for (script in scripts) {
                                    array.add(script)
                                }

                                vertx.eventBus().send<String>("og-liability-tracker.db_script_runner", JsonObject().put("scripts", array).encode()) { reply ->
                                    if (reply.failed())
                                        throw reply.cause()

                                    dbPopulatedFuture.complete()
                                }
                            } else {
                                dbPopulatedFuture.complete()
                            }
                        }

                        dbPopulatedFuture.setHandler {
                            if (it.failed())
                                throw it.cause()

                            vertx.deployVerticle(ApiServer(), apiDeploymentOptions) {
                                if (it.failed())
                                    throw it.cause()

                                startFuture.complete()

                                connection.close()
                            }
                        }
                    }
                }
            }
        } catch (t : Throwable) {
            startFuture.fail(t)
        }
    }

    private fun deployWorker(verticle : AbstractVerticle, options : DeploymentOptions) : Future<Any> {
        val future = Future.future<Any>()

        vertx.deployVerticle(verticle, options) {
            if (it.failed())
                future.fail(it.cause())
            else
                future.complete()
        }

        return future
    }
}