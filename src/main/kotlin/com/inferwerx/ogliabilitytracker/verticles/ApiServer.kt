package com.inferwerx.ogliabilitytracker.verticles

import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.eventbus.EventBus
import io.vertx.core.http.HttpHeaders
import io.vertx.core.http.HttpServerResponse
import io.vertx.core.json.Json
import io.vertx.core.json.JsonObject
import io.vertx.ext.jdbc.JDBCClient
import io.vertx.ext.sql.SQLConnection
import io.vertx.ext.web.Router
import io.vertx.ext.web.handler.BodyHandler
import io.vertx.ext.web.handler.StaticHandler
import org.h2.jdbc.JdbcSQLException
import java.net.ServerSocket

class ApiServer : AbstractVerticle() {
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

        vertx.createHttpServer().requestHandler({ router.accept(it) }).listen(listenPort) {
            if (it.succeeded()) {
                println("API Deployed: http://localhost:$listenPort")

                startFuture.complete()
            } else {
                println("API Failed: ${it.cause().message}")

                startFuture.fail(it.cause())
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
     * An extension added to HttpServerResponse class that makes responding with JSON a little less verbose
     */
    fun HttpServerResponse.endWithJson(obj: Any) {
        this.putHeader(HttpHeaders.CONTENT_TYPE, "application/json").end(Json.encode(obj))
    }
}