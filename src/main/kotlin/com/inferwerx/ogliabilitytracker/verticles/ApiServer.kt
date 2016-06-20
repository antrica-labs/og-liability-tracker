package com.inferwerx.ogliabilitytracker.verticles

import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.Handler
import io.vertx.core.eventbus.DeliveryOptions
import io.vertx.core.eventbus.EventBus
import io.vertx.core.http.HttpHeaders
import io.vertx.core.http.HttpServerResponse
import io.vertx.core.json.Json
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.ext.jdbc.JDBCClient
import io.vertx.ext.sql.SQLConnection
import io.vertx.ext.web.Router
import io.vertx.ext.web.RoutingContext
import io.vertx.ext.web.handler.BodyHandler
import io.vertx.ext.web.handler.StaticHandler
import org.h2.jdbc.JdbcSQLException
import java.net.ServerSocket
import java.util.*

class ApiServer : AbstractVerticle() {
    private val dbTestQuery = "SELECT * FROM provinces"

    override fun start(startFuture : Future<Void>) {
        val dbConfig = JsonObject()
                .put("driver_class", config().getString("db.jdbc_driver"))
                .put("url", "${config().getString("db.url_proto")}${config().getString("db.file_path")}${config().getString("db.url_options")}")
                .put("user", config().getString("db.username"))
                .put("password", config().getString("db.password"))
        val dbClient = JDBCClient.createShared(vertx, dbConfig)
        val eventBus = vertx.eventBus()

        val router = createRoutes(dbClient, eventBus)
        val listenPort = getRandomizedPort()

        val dbSetupFuture = Future.future<Void>()

        // Check and setup the database if it doesn't exist
        dbClient.getConnection {
            if (it.failed()) {
                dbSetupFuture.fail(it.cause())

                return@getConnection
            }

            val connection = it.result()

            connection.query(dbTestQuery) { query ->
                if (query.failed()) {
                    // A table likely doesn't exist, indicating that the database objects haven't been created
                    val createScript = config().getString("db.create_script")
                    vertx.eventBus().send<String>("og-liability-tracker.db_script_runner", createScript) { reply ->
                        if (reply.failed())
                            dbSetupFuture.fail(reply.cause())
                        else
                            dbSetupFuture.complete()
                    }
                } else {
                    // If there are result returned from the test query, we assume the database is in working order
                    if (query.result().numRows > 0)
                        dbSetupFuture.complete()
                    else
                        dbSetupFuture.fail(Throwable("Database test query returned zero results [$dbTestQuery]"))
                }
            }
        }

        // We shouldn't start listening for requests until we can actually complete them, which means the DB needs to be
        // ready and in a sane state.
        dbSetupFuture.setHandler {
            if (it.failed()) {
                startFuture.fail(it.cause())

                return@setHandler
            }

            // The database should be ready to go now, so start listening for requests
            vertx.createHttpServer().requestHandler({ router.accept(it) }).listen(listenPort) {
                if (it.failed()) {
                    startFuture.fail(it.cause())

                    return@listen
                }

                println("API Deployed: http://localhost:$listenPort")

                startFuture.complete()
            }
        }
    }

    /**
     * When deploying as an electron application it's impossible to know what ports will be available for the API server
     * so this function can be used to select a random port.
     *
     * Note: It is possible that a port could be taken in the time between this function returning and the API server
     * starting. Highly unlikely, but possible.
     */
    private fun getRandomizedPort() : Int {
        val socket = ServerSocket(0);

        val port = socket.localPort
        socket.close();

        return port
    }

    /**
     * This is where the API routes are setup. This function gets called when the verticle starts.
     */
    private fun createRoutes(dbClient : JDBCClient, eventBus : EventBus) = Router.router(vertx).apply {
        // Add a body handler so that file uploads work
        route().handler(BodyHandler.create())

        // Most routes under the '/api/' path will need access to the jdbc client and event bus, so we might as well
        // reduce some of that initialization code by including it in the context of the handler automatically.
        //
        // The benefit of this is that we can also ensure that the connection is closed when the route handler completes.
        route("/api/*").handler { context ->
            context.put("eventbus", eventBus)
            context.put("dbclient", dbClient)

            dbClient.getConnection {
                if (it.failed())
                    context.fail(it.cause())
                else {
                    val connection = it.result()

                    context.put("dbconnection", connection)

                    context.addHeadersEndHandler {
                        connection.close()
                    }

                    context.next()
                }
            }
        }.failureHandler { context ->
            val connection = context.get<SQLConnection>("dbconnection")

            connection.close()

            sendError(500, context.response())
        }

        route("/api/upload_ab_liabilities").handler(handleLiabilityUpload)

        // Serves static files out of the 'webroot' folder
        route("/pub/*").handler(StaticHandler.create().setCachingEnabled(false))

        // Redirect to the static files by default
        get("/").handler { context ->
            context.response().setStatusCode(302).putHeader(HttpHeaders.LOCATION, "/pub/").end()
        }
    }

    /**
     * A quick and easy way to send error messages as a response.
     */
    private fun sendError(statusCode: Int, response: HttpServerResponse, error: Throwable? = null) {
        response.statusCode = statusCode;

        val message = JsonObject()

        if (error != null && error is JdbcSQLException) {
            message.put("error", error.originalMessage)
        } else if (error != null) {
            message.put("error", error.message)
        } else {
            message.put("error", statusCode)
        }

        response.endWithJson(message)
    }

    /**
     * Route handlers
     */

    /**
     * One or more DDS files can be uploaded at a time. A company name must also be specified.
     */
    val handleLiabilityUpload = Handler<RoutingContext> { context ->
        val eb = context.get<EventBus>("eventbus")

        val importMessages = JsonArray()

        context.fileUploads().forEach {
            val message = JsonObject()
                    .put("fileName", it.fileName())
                    .put("size", it.size())
                    .put("contentType", it.contentType())
                    .put("uploadedFileName", it.uploadedFileName())


            eb.send<String>("og-liability-tracker.ab_importer", message.encode()) { reply ->
                if (reply.succeeded()) {
                    importMessages.add(JsonObject(reply.result().body().toString()))
                } else {
                    throw Exception(reply.cause())
                }
            }
        }

        context.response().endWithJson(importMessages)
    }

    /**
     * An extension added to HttpServerResponse class that makes responding with JSON a little less verbose
     */
    fun HttpServerResponse.endWithJson(obj: Any) {
        this.putHeader(HttpHeaders.CONTENT_TYPE, "application/json").end(Json.encode(obj))
    }
}