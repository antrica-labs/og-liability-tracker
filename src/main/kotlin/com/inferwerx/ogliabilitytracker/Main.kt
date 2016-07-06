package com.inferwerx.ogliabilitytracker

import com.inferwerx.ogliabilitytracker.exceptions.MultiAsyncException
import com.inferwerx.ogliabilitytracker.verticles.ApiServer
import com.inferwerx.ogliabilitytracker.verticles.workers.*
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

        val dbScriptRunnerFuture = Future.future<Void>() // Used to determine if the db script runner verticle started
        val importStartFuture = Future.future<Void>()    // Used to determine if the liability importer verticles started
        val hierarchyMapperFuture = Future.future<Void>()
        val forecasterFuture = Future.future<Void>()
        var exporterFuture = Future.future<Void>()

        val apiDeploymentOptions = DeploymentOptions().setConfig(config())
        val workerDeploymentOptions = DeploymentOptions().setWorker(true).setConfig(config())

        vertx.deployVerticle(DatabaseScriptRunner(), workerDeploymentOptions) {
            if (it.failed())
                dbScriptRunnerFuture.fail(it.cause())
            else
                dbScriptRunnerFuture.complete()
        }

        vertx.deployVerticle(AlbertaLiabilityImporter(), workerDeploymentOptions) {
            if (it.failed())
                importStartFuture.fail(it.cause())
            else
                importStartFuture.complete()
        }

        vertx.deployVerticle(HierarchyImporter(), workerDeploymentOptions) {
            if (it.failed())
                hierarchyMapperFuture.fail(it.cause())
            else
                hierarchyMapperFuture.complete()
        }

        vertx.deployVerticle(LiabilityForecaster(), workerDeploymentOptions) {
            if (it.failed())
                forecasterFuture.fail(it.cause())
            else
                forecasterFuture.complete()
        }

        vertx.deployVerticle(LiabilityExporter(), workerDeploymentOptions) {
            if (it.failed())
                exporterFuture.fail(it.cause())
            else
                exporterFuture.complete()
        }

        try {
            // The API server should only start if the worker verticles started
            CompositeFuture.all(dbScriptRunnerFuture, importStartFuture, hierarchyMapperFuture, forecasterFuture, exporterFuture).setHandler {
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
}