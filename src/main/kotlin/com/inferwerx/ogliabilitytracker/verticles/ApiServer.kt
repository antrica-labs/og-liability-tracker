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
import java.io.File
import java.net.ServerSocket
import java.nio.file.Files
import java.text.SimpleDateFormat
import java.time.Instant
import java.util.*

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
        val listenPort = config().getInteger("http.port") ?: getRandomizedPort()

        vertx.createHttpServer().requestHandler({ router.accept(it) }).listen(listenPort) {
            if (it.succeeded()) {
                println("API Deployed: http://localhost:$listenPort")

                startFuture.complete()
            } else {
                println("API Deployment Failed: ${it.cause()}")

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

        // Setup routes
        route("/api/companies").handler(handleGetCompanies)
        route("/api/provinces").handler(handleGetProvinces)
        route("/api/historical_liabilities").handler(handleHistoricalRatings)
        route("/api/pro_forma_liabilities").handler(handleProFormaRatings)
        route("/api/upload_ab_liabilities").handler(handleAbLiabilityUpload)

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
     * Responds with a list of all of the provinces that exist in the database
     */
    val handleGetProvinces = Handler<RoutingContext> { context ->
        val db = context.get<SQLConnection>("dbconnection")
        val sql = "SELECT id, name, short_name FROM provinces ORDER BY name"

        db.query(sql) { query ->
            if (query.failed())
                sendError(500, context.response(), query.cause())
            else
                context.response().endWithJson(query.result().toJson().getJsonArray("rows"))
        }
    }

    /**
     * Responds with a list of all companies that are in the database
     */
    val handleGetCompanies = Handler<RoutingContext> { context ->
        val db = context.get<SQLConnection>("dbconnection")
        val sql = "SELECT id, name, alt_name FROM companies ORDER BY name"

        db.query(sql) { query ->
            if (query.failed())
                sendError(500, context.response(), query.cause())
            else
                context.response().endWithJson(query.result().toJson().getJsonArray("rows"))
        }
    }

    /**
     * Returns the **pro forma** LLR ratings for a given province and company. In this context, pro forma means that
     * only licences that are currently held by the selected company are included in the historical data. This is good
     * for when you want to calculate asset value decline.
     *
     * Parameters:
     * company_id - Integer ID of a company
     * province_id - Integer ID of a province
     * start_date - String representation of the earliest LLR rating to select (yyyy-mm-dd)
     * end_date - String representation of the latest LLR rating to select (yyyy-mm-dd)
     */
    val handleProFormaRatings = Handler<RoutingContext> { context ->
        val db = context.get<SQLConnection>("dbconnection")
        val query = """
        SELECT
          date(r.report_month, 'unixepoch') AS report_month,
          sum(r.asset_value)                AS asset_value,
          sum(r.liability_value)            AS liability_value
        FROM entity_ratings r INNER JOIN entities e ON e.id = r.entity_id
        WHERE e.id IN (
          SELECT entity_id
          FROM entity_ratings
          WHERE report_month IN (SELECT max(report_month) latest_month
                                 FROM entity_ratings))
              AND e.province_id = ?
              AND e.company_id = ?
              AND r.report_month >= ?
              AND r.report_month <= ?
        GROUP BY r.report_month
        ORDER BY r.report_month
        """

        // We could do all of this without creating so many variables, but this is helpful when debugging and probably
        // gets optimized away during runtime anyway...
        val province = context.request().getParam("province_id")
        val company = context.request().getParam("company_id")
        val dateFormat = SimpleDateFormat("yyyy-MM-dd", Locale.CANADA)
        val startDateStr = context.request().getParam("start_date")
        val endDateStr = context.request().getParam("end_date")
        val startDate = dateFormat.parse(startDateStr)
        val endDate = dateFormat.parse(endDateStr)

        val params = JsonArray()

        params.add(province.toInt())
        params.add(company.toInt())
        params.add(startDate.time / 1000)
        params.add(endDate.time / 1000)

        db.queryWithParams(query, params) { query ->
            if (query.failed())
                sendError(500, context.response(), query.cause())
            else
                context.response().endWithJson(query.result().toJson().getJsonArray("rows"))
        }
    }


    /**
     * Returns the ass-is historical LLR ratings for a given province and company.
     *
     * Parameters:
     * company_id - Integer ID of a company
     * province_id - Integer ID of a province
     * start_date - String representation of the earliest LLR rating to select (yyyy-mm-dd)
     * end_date - String representation of the latest LLR rating to select (yyyy-mm-dd)
     */
    val handleHistoricalRatings = Handler<RoutingContext> { context ->
        val db = context.get<SQLConnection>("dbconnection")
        val query = """
        SELECT
          date(r.report_month, 'unixepoch') AS report_month,
          sum(r.asset_value)                AS asset_value,
          sum(r.liability_value)            AS liability_value
        FROM entity_ratings r INNER JOIN entities e ON e.id = r.entity_id
        WHERE e.province_id = ?
              AND e.company_id = ?
              AND r.report_month >= ?
              AND r.report_month <= ?
        GROUP BY r.report_month
        ORDER BY r.report_month
        """

        // We could do all of this without creating so many variables, but this is helpful when debugging and probably
        // gets optimized away during runtime anyway...
        val province = context.request().getParam("province_id")
        val company = context.request().getParam("company_id")
        val dateFormat = SimpleDateFormat("yyyy-MM-dd", Locale.CANADA)
        val startDateStr = context.request().getParam("start_date")
        val endDateStr = context.request().getParam("end_date")
        val startDate = dateFormat.parse(startDateStr)
        val endDate = dateFormat.parse(endDateStr)

        val params = JsonArray()

        params.add(province.toInt())
        params.add(company.toInt())
        params.add(startDate.time / 1000)
        params.add(endDate.time / 1000)

        db.queryWithParams(query, params) { query ->
            if (query.failed())
                sendError(500, context.response(), query.cause())
            else
                context.response().endWithJson(query.result().toJson().getJsonArray("rows"))
        }
    }

    /**
     * One or more DDS files can be uploaded at a time. A company name must also be specified.
     */
    val handleAbLiabilityUpload = Handler<RoutingContext> { context ->
        val eb = context.get<EventBus>("eventbus")

        val message = JsonObject()
        val uploadedFiles = JsonArray()

        message.put("append", context.request().formAttributes().get("append") == "on")
        message.put("company", context.request().formAttributes().get("company_id").toInt())

        context.fileUploads().forEach {
            val file = JsonObject()

            file.put("fileName", it.uploadedFileName())
            file.put("originalFileName", it.fileName())
            file.put("size", it.size())
            file.put("contentType", it.contentType())

            uploadedFiles.add(file)
        }

        message.put("uploadedFiles", uploadedFiles)

        // Send message to the importer worker with a 2 minute timeout
        eb.send<String>("og-liability-tracker.ab_importer", message.encode(), DeliveryOptions().setSendTimeout(120000)) { reply ->
            if (reply.succeeded())
                context.response().endWithJson(JsonObject(reply.result().body()))
            else
                context.response().endWithJson(JsonObject().put("status", "failed").put("message", reply.cause().toString()))

            context.fileUploads().forEach {
                try {
                    Files.delete(File(it.uploadedFileName()).toPath())
                } catch (t : Throwable) {
                    System.err.println(t.message)
                }
            }
        }
    }

    /**
     * An extension added to HttpServerResponse class that makes responding with JSON a little less verbose
     */
    fun HttpServerResponse.endWithJson(obj: Any) {
        this.putHeader(HttpHeaders.CONTENT_TYPE, "application/json").end(Json.encode(obj))
    }
}