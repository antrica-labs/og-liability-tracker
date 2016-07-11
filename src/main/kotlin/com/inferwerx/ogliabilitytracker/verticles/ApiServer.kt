package com.inferwerx.ogliabilitytracker.verticles

import com.inferwerx.ogliabilitytracker.queries.InternalQueries
import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.Handler
import io.vertx.core.eventbus.DeliveryOptions
import io.vertx.core.eventbus.EventBus
import io.vertx.core.http.HttpHeaders
import io.vertx.core.http.HttpMethod
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
import java.net.InterfaceAddress
import java.net.ServerSocket
import java.nio.file.Files
import java.nio.file.Paths
import java.text.SimpleDateFormat
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

        // Setup APIs
        route(HttpMethod.GET,  "/api/provinces").handler(handleGetProvinces)
        route(HttpMethod.GET,  "/api/base_lmr_history").handler(handleHistoricalRatings)
        route(HttpMethod.GET,  "/api/base_pro_forma_lmr_history").handler(handleProFormaRatings)
        route(HttpMethod.GET,  "/api/base_lmr_forecast").handler(handleBaseForecast)
        route(HttpMethod.GET,  "/api/combined_lmr_forecast").handler(handleFullInclusionForecast)
        route(HttpMethod.GET,  "/api/base_report_dates").handler(handleReportDates)
        route(HttpMethod.GET,  "/api/historical_netbacks").handler(handleHistoricalNetbacks)
        route(HttpMethod.GET,  "/api/lmr_details").handler(handleLiabilityDetails)
        route(HttpMethod.POST, "/api/upload_ab_liabilities").handler(handleAbLiabilityUpload)
        route(HttpMethod.POST, "/api/upload_hierarchy_mapping").handler(handleHierarchyMappingUpload)
        route(HttpMethod.POST, "/api/export_historical_report").handler(handleExportLiabilities)
        route(HttpMethod.GET,  "/api/dispositions").handler(handleGetDispositions)
        route(HttpMethod.POST, "/api/create_disposition").handler(handleCreateDisposition)
        route(HttpMethod.POST, "/api/delete_disposition").handler(handleDeleteDisposition)
        route(HttpMethod.GET,  "/api/acquisitions").handler(handleGetAcquisitions)
        route(HttpMethod.POST, "/api/create_acquisition").handler(handleCreateAcquisition)
        route(HttpMethod.POST, "/api/delete_acquisition").handler(handleDeleteAcquisition)
        route(HttpMethod.GET,  "/api/acquisition_lmr_history").handler(handleAcquisitionRatings)
        route(HttpMethod.GET,  "/api/acquisition_lmr_forecast").handler(handleForecastAcquisition)
        route(HttpMethod.GET,  "/api/aro_plans").handler(handleGetAroPlans)
        route(HttpMethod.POST, "/api/create_aro_plan").handler(handleCreateAroPlan)
        route(HttpMethod.POST, "/api/delete_aro_plan").handler(handleDeleteAroPlan)

        // Serves static files out of the 'webroot' folder
        route("/pub/*").handler(StaticHandler.create().setCachingEnabled(false))

        // Redirect to the static files by default
        get("/").handler { context ->
            context.response().setStatusCode(302).putHeader(HttpHeaders.LOCATION, "/pub/").end()
        }
    }

    /**
     * An extension added to HttpServerResponse class that makes responding with JSON a little less verbose
     */
    fun HttpServerResponse.endWithJson(obj: Any) {
        this.putHeader(HttpHeaders.CONTENT_TYPE, "application/json").end(Json.encode(obj))
    }

    /**
     * When deploying as an electron application it's impossible to know what ports will be available for the API server
     * so this function can be used to select a random port.
     *
     * Note: It is possible that a port could be taken in the time between this function returning and the API server
     * starting. Highly unlikely, but possible.
     */
    private fun getRandomizedPort() : Int {
        val socket = ServerSocket(0)

        val port = socket.localPort
        socket.close()

        return port
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
     * Responds with a list of all of the provinces that exist in the database
     */
    val handleGetProvinces = Handler<RoutingContext> { context ->
        val db = context.get<SQLConnection>("dbconnection")

        db.query(InternalQueries.GET_ALL_PROVINCES) { query ->
            if (query.failed())
                sendError(500, context.response(), query.cause())
            else
                context.response().endWithJson(query.result().rows)
        }
    }

    /**
     * Returns the **pro forma** LLR ratings for a given province. In this context, pro forma means that
     * only licences that are currently held are included in the historical data. This is good
     * for when you want to calculate asset value decline.
     *
     * Parameters:
     * province_id - Integer ID of a province
     */
    val handleProFormaRatings = Handler<RoutingContext> { context ->
        val db = context.get<SQLConnection>("dbconnection")

        val province = context.request().getParam("province_id").toInt()

        val params = JsonArray()

        params.add(province)
        params.add(province)

        db.queryWithParams(InternalQueries.GET_PROFORMA_HISTORY, params) { query ->
            if (query.failed())
                sendError(500, context.response(), query.cause())
            else
                context.response().endWithJson(query.result().rows)
        }
    }

    /**
     * Returns the as-is historical LLR ratings for a given province.
     *
     * Parameters:
     * province_id - Integer ID of a province
     * start_date - String representation of the earliest LLR rating to select (yyyy-mm-dd)
     * end_date - String representation of the latest LLR rating to select (yyyy-mm-dd)
     */
    val handleHistoricalRatings = Handler<RoutingContext> { context ->
        val db = context.get<SQLConnection>("dbconnection")

        val province = context.request().getParam("province_id")

        val params = JsonArray()

        params.add(province.toInt())

        db.queryWithParams(InternalQueries.GET_HISTORY, params) { query ->
            if (query.failed())
                sendError(500, context.response(), query.cause())
            else
                context.response().endWithJson(query.result().rows)
        }
    }

    /**
     * Returns a list of all months that have liability data
     *
     * Parameters:
     * province_id - Integer ID of a province
     */
    val handleReportDates = Handler<RoutingContext> { context ->
        val db = context.get<SQLConnection>("dbconnection")

        val province = context.request().getParam("province_id")

        val params = JsonArray()

        params.add(province.toInt())

        db.queryWithParams(InternalQueries.GET_REPORT_DATES, params) { query ->
            if (query.failed())
                sendError(500, context.response(), query.cause())
            else
                context.response().endWithJson(query.result().rows)
        }
    }

    /**
     * Gets the netbacks that used historically by province. This is useful for trying to back
     * calculate volumes from asset value
     *
     * Parameters:
     * province_id - Integer ID of a province
     */
    val handleHistoricalNetbacks = Handler<RoutingContext> { context ->
        val db = context.get<SQLConnection>("dbconnection")

        val province = context.request().getParam("province_id")

        val params = JsonArray()

        params.add(province.toInt())

        db.queryWithParams(InternalQueries.GET_NETBACKS, params) { query ->
            if (query.failed())
                sendError(500, context.response(), query.cause())
            else
                context.response().endWithJson(query.result().rows)
        }
    }

    /**
     * Gets the liability details for a specified report date. In order to know what report dates are available, you
     * may make a call to /api/report_dates.
     *
     * Parameters:
     * province_id - Integer ID of a province
     * report_date - String representation of the report date that details are requested for (yyyy-mm-dd)
     */
    val handleLiabilityDetails = Handler<RoutingContext> { context ->
        val db = context.get<SQLConnection>("dbconnection")


        val dateFormat = SimpleDateFormat("yyyy-MM-dd", Locale.CANADA)
        val province = context.request().getParam("province_id")
        val reportMonth = dateFormat.parse(context.request().getParam("report_date")).toInstant()

        val params = JsonArray()

        params.add(reportMonth)
        params.add(province.toInt())

        db.queryWithParams(InternalQueries.GET_REPORT_DETAILS, params) { query ->
            if (query.failed())
                sendError(500, context.response(), query.cause())
            else {
                context.response().endWithJson(query.result().rows)
            }
        }
    }

    /**
     * One or more DDS files can be uploaded at a time. This call is for uploading Alberta LLR only.
     *
     * Parameters (in addition to uploaded files):
     * append - If 'on' then LLR data will be appended to any exiting data, otherwise all LLR data for the company
     *          will be cleared before importing the new data. This is handy for when you want to re-upload everything,
     *          or when just add additional months as they become available.
     */
    val handleAbLiabilityUpload = Handler<RoutingContext> { context ->
        val eb = context.get<EventBus>("eventbus")

        val message = JsonObject()
        val uploadedFiles = JsonArray()

        message.put("append", context.request().formAttributes().get("append") == "on")

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
     * Handles the upload of a hierarchy mapping file, formatted like /webroot/resources/sample-hierarchy-update.csv     *
     */
    val handleHierarchyMappingUpload = Handler<RoutingContext> { context ->
        val eb = context.get<EventBus>("eventbus")
        val future = Future.future<Void>()

        if (context.fileUploads().count() > 0) {
            val upload = context.fileUploads().toTypedArray()[0]
            val message = JsonObject().put("filename", upload.uploadedFileName())

            eb.send<String>("og-liability-tracker.hierarchy_importer", message.encode(), DeliveryOptions().setSendTimeout(120000)) { reply ->
                if (reply.succeeded()) {
                    context.response().endWithJson(JsonObject(reply.result().body()))

                    future.complete()
                } else {
                    context.response().endWithJson(JsonObject().put("status", "failed").put("message", reply.cause().toString()))

                    future.complete()
                }
            }
        } else {
            future.complete()
        }


        future.setHandler {
            context.fileUploads().forEach {
                try {
                    Files.delete(Paths.get(it.uploadedFileName()))
                } catch (t : Throwable) {
                    System.err.println(t.message)
                }
            }
        }
    }

    /**
     * Handles the setup of disposition senarios. Includes a CSV file upload like /webroot/resources/sample-dispostion-list.csv
     */
    val handleCreateDisposition = Handler<RoutingContext> { context ->
        val eb = context.get<EventBus>("eventbus")
        val future = Future.future<Void>()

        val upload = context.fileUploads().toTypedArray()[0]
        val description = context.request().getParam("description")
        val salePrice = context.request().getParam("sale_price").toDouble()
        val province = context.request().getParam("province_id").toInt()
        val dateFormat = SimpleDateFormat("yyyy-MM-dd", Locale.CANADA)
        val effectiveDate = dateFormat.parse(context.request().getParam("effective_date")).toInstant()

        val message = JsonObject()

        message.put("description", description)
        message.put("effective_date", effectiveDate)
        message.put("sale_price", salePrice)
        message.put("province_id", province)
        message.put("filename", upload.uploadedFileName())

        eb.send<String>("og-liability-tracker.disposition_importer", message.encode(), DeliveryOptions().setSendTimeout(120000)) { reply ->
            if (reply.succeeded()) {
                context.response().endWithJson(JsonObject(reply.result().body()))

                future.complete()
            } else {
                context.response().endWithJson(JsonObject().put("status", "failed").put("message", reply.cause().toString()))

                future.complete()
            }
        }

        future.setHandler {
            context.fileUploads().forEach {
                try {
                    Files.delete(Paths.get(it.uploadedFileName()))
                } catch (t : Throwable) {
                    System.err.println(t.message)
                }
            }
        }
    }

    val handleGetDispositions = Handler<RoutingContext> { context ->
        val db = context.get<SQLConnection>("dbconnection")
        val params = JsonArray().add(context.request().getParam("province_id").toInt())

        db.queryWithParams(InternalQueries.GET_DISPOSITIONS, params) {
            if (it.failed())
                sendError(500, context.response(), it.cause())
            else
                context.response().endWithJson(it.result().rows)
        }
    }

    val handleDeleteDisposition = Handler<RoutingContext> { context ->
        val db = context.get<SQLConnection>("dbconnection")
        val params = JsonArray()

        val id = context.request().formAttributes().get("id")

        params.add(id.toInt())

        db.updateWithParams(InternalQueries.DELETE_DISPOSITION_ENTITIES, params) {
            if (it.succeeded())
                db.updateWithParams(InternalQueries.DELETE_DISPOSITION, params) {
                    if (it.succeeded())
                        context.response().endWithJson(JsonObject().put("status", "success"))
                    else
                        sendError(500, context.response(), it.cause())
                }
            else
                sendError(500, context.response(), it.cause())
        }
    }

    val handleCreateAcquisition = Handler<RoutingContext> { context ->
        val eb = context.get<EventBus>("eventbus")
        val future = Future.future<Void>()

        val upload = context.fileUploads().toTypedArray()[0]
        val description = context.request().getParam("description")
        val purchasePrice = context.request().getParam("purchase_price").toDouble()
        val province = context.request().getParam("province_id").toInt()
        val dateFormat = SimpleDateFormat("yyyy-MM-dd", Locale.CANADA)
        val effectiveDate = dateFormat.parse(context.request().getParam("effective_date")).toInstant()

        val message = JsonObject()

        message.put("description", description)
        message.put("effective_date", effectiveDate)
        message.put("purchase_price", purchasePrice)
        message.put("province_id", province)
        message.put("filename", upload.uploadedFileName())

        eb.send<String>("og-liability-tracker.acquisition_importer", message.encode(), DeliveryOptions().setSendTimeout(120000)) { reply ->
            if (reply.succeeded()) {
                context.response().endWithJson(JsonObject(reply.result().body()))

                future.complete()
            } else {
                context.response().endWithJson(JsonObject().put("status", "failed").put("message", reply.cause().toString()))

                future.complete()
            }
        }

        future.setHandler {
            context.fileUploads().forEach {
                try {
                    Files.delete(Paths.get(it.uploadedFileName()))
                } catch (t : Throwable) {
                    System.err.println(t.message)
                }
            }
        }
    }

    val handleGetAcquisitions = Handler<RoutingContext> { context ->
        val db = context.get<SQLConnection>("dbconnection")
        val params = JsonArray().add(context.request().getParam("province_id").toInt())

        db.queryWithParams(InternalQueries.GET_ACQUISITIONS, params) {
            if (it.failed())
                sendError(500, context.response(), it.cause())
            else
                context.response().endWithJson(it.result().rows)
        }
    }

    val handleDeleteAcquisition = Handler<RoutingContext> { context ->
        val db = context.get<SQLConnection>("dbconnection")
        val params = JsonArray()

        val id = context.request().formAttributes().get("id")

        params.add(id.toInt())

        db.updateWithParams(InternalQueries.DELETE_ACQUISITION_LICENCES, params) {
            if (it.succeeded())
                db.updateWithParams(InternalQueries.DELETE_ACQUISITION, params) {
                    if (it.succeeded())
                        context.response().endWithJson(JsonObject().put("status", "success"))
                    else
                        sendError(500, context.response(), it.cause())
                }
            else
                sendError(500, context.response(), it.cause())
        }
    }

    val handleAcquisitionRatings = Handler<RoutingContext> { context ->
        val db = context.get<SQLConnection>("dbconnection")
        val eb = context.get<EventBus>("eventbus")
        val params = JsonArray()

        params.add(context.request().getParam("id").toInt())

        try {
            db.queryWithParams(InternalQueries.GET_ACQUISITION_NETBACKS, params) { netbacks ->
                if (netbacks.failed())
                    throw netbacks.cause()

                val message = JsonObject()

                message.put("netbacks", netbacks.result().rows)

                db.queryWithParams(InternalQueries.GET_ACQUISITION_LICENCES, params) { licences ->
                    if (licences.failed())
                        throw licences.cause()

                    message.put("licences", licences.result().rows)

                    eb.send<String>("og-liability-tracker.public_data_history", message.encode(), DeliveryOptions().setSendTimeout(120000)) { reply ->
                        if (reply.succeeded())
                            context.response().endWithJson(JsonArray(reply.result().body()))
                        else
                            throw reply.cause()
                    }
                }
            }
        } catch (t : Throwable) {
            sendError(500, context.response(), t)
        }
    }

    val handleForecastAcquisition = Handler<RoutingContext> { context ->
        val db = context.get<SQLConnection>("dbconnection")
        val eb = context.get<EventBus>("eventbus")
        val params = JsonArray()

        params.add(context.request().getParam("id").toInt())

        try {
            db.queryWithParams(InternalQueries.GET_ACQUISITION_NETBACKS, params) { netbacks ->
                if (netbacks.failed())
                    throw netbacks.cause()

                val message = JsonObject()

                message.put("netbacks", netbacks.result().rows)

                db.queryWithParams(InternalQueries.GET_ACQUISITION_LICENCES, params) { licences ->
                    if (licences.failed())
                        throw licences.cause()

                    message.put("licences", licences.result().rows)

                    eb.send<String>("og-liability-tracker.public_data_history", message.encode(), DeliveryOptions().setSendTimeout(120000)) { reply ->
                        if (reply.failed())
                            throw reply.cause()

                        val forecastMessage = JsonObject()

                        forecastMessage.put("netbacks", netbacks.result().rows)
                        forecastMessage.put("historical_lmr", JsonArray(reply.result().body()))

                        eb.send<String>("og-liability-tracker.simple_forecaster", forecastMessage.encode(), DeliveryOptions().setSendTimeout(120000)) { forecastReply ->
                            if (forecastReply.succeeded()) {
                                context.response().endWithJson(JsonArray(forecastReply.result().body()))
                            } else {
                                throw forecastReply.cause()
                            }
                        }
                    }
                }
            }
        } catch (t : Throwable) {
            sendError(500, context.response(), t)
        }
    }

    /**
     * Forecasts LMR ratings in the future using historical LMR ratings. The forecast starts one month after the last
     * month that an LMR report exists for. This forecast will include dispositions, the only way to forecast their
     * impact is to remove them from historical reports.
     *
     * Parameters:
     * province_id - Integer ID of a province
     */
    val handleBaseForecast = Handler<RoutingContext> { context ->
        val eb = context.get<EventBus>("eventbus")
        val db = context.get<SQLConnection>("dbconnection")

        val province = context.request().getParam("province_id").toInt()

        val netbackParams = JsonArray()
        netbackParams.add(province)

        val lmrParams = JsonArray()
        lmrParams.add(province)
        lmrParams.add(province)

        try {
            db.queryWithParams(InternalQueries.GET_NETBACKS, netbackParams) { netback ->
                if (netback.failed())
                    throw Throwable(netback.cause())

                val message = JsonObject()

                message.put("netbacks", netback.result().rows)

                db.queryWithParams(InternalQueries.GET_PROFORMA_HISTORY, lmrParams) { lmr ->
                    if (lmr.failed())
                        throw Throwable(lmr.cause())

                    message.put("historical_lmr", lmr.result().rows)

                    eb.send<String>("og-liability-tracker.simple_forecaster", message.encode(), DeliveryOptions().setSendTimeout(120000)) { reply ->
                        if (reply.succeeded()) {
                            context.response().endWithJson(JsonArray(reply.result().body()))
                        } else {
                            throw reply.cause()
                        }
                    }
                }
            }
        } catch (t : Throwable) {
            sendError(500, context.response(), t)
        }
    }

    /**
     * The difference between the simple forecast and this combined forecast is the layering in of growth, acquisitions,
     * remediation and dispositions.
     *
     * Parameters:
     * province_id - Integer ID of a province
     */
    val handleFullInclusionForecast = Handler<RoutingContext> { context ->

    }

    /**
     * Exports liability details to a supplied JXLS template
     *
     * Parameters (in addition to the template file upload):
     * province_id - Integer ID of a province
     * report_date - String representation of the report date that details are requested for (yyyy-mm-dd)
     */
    val handleExportLiabilities = Handler<RoutingContext> { context ->
        val eb = context.get<EventBus>("eventbus")
        val future = Future.future<String>()

        if (context.fileUploads().count() > 0) {
            val upload = context.fileUploads().toTypedArray()[0]
            val province = context.request().getParam("province_id")
            val reportDateStr = context.request().getParam("report_date")
            val message = JsonObject()
                    .put("province", province.toInt())
                    .put("report_date", reportDateStr)
                    .put("filename", upload.uploadedFileName())
                    .put("originalFilename", upload.fileName())

            eb.send<String>("og-liability-tracker.liability_exporter", message.encode(), DeliveryOptions().setSendTimeout(120000)) { reply ->
                if (reply.succeeded()) {
                    future.complete(reply.result().body())
                } else {
                    future.fail(reply.cause())
                }
            }
        } else {
            future.complete()
        }

        future.setHandler {
            context.fileUploads().forEach {
                try {
                    Files.delete(Paths.get(it.uploadedFileName()))
                } catch (t : Throwable) {
                    System.err.println(t.message)
                }
            }

            if (it.failed()) {
                sendError(500, context.response(), Throwable(it.cause()))
            } else {
                val exportFile = it.result()

                val path = Paths.get(exportFile)

                context.response().putHeader("Content-Disposition", "filename=\"${path.fileName.toString()}\"")
                context.response().sendFile(exportFile) {
                    try {
                        Files.delete(path)
                    } catch (t : Throwable) {
                        System.err.println(t.message)
                    }
                }
            }
        }
    }

    val handleCreateAroPlan = Handler<RoutingContext> { context ->
        val db = context.get<SQLConnection>("dbconnection")
        val params = JsonArray()
        val dateFormat = SimpleDateFormat("yyyy-MM-dd", Locale.CANADA)

        val province = context.request().formAttributes().get("province_id").toInt()
        val description = context.request().formAttributes().get("description")
        val effectiveDate = dateFormat.parse(context.request().getParam("effective_date")).toInstant()
        val reductionAmount = context.request().formAttributes().get("reduction_amount").toDouble()
        val cost = context.request().formAttributes().get("cost").toDouble()
        val comments = context.request().formAttributes().get("comments")

        params.add(province)
        params.add(true)
        params.add(description)
        params.add(effectiveDate)
        params.add(reductionAmount)
        params.add(cost)
        params.add(comments)

        db.updateWithParams(InternalQueries.INSERT_ARO_PLAN, params) {
            if (it.succeeded())
                context.response().endWithJson(JsonObject().put("status", "success").put("results", it.result().keys))
            else
                sendError(500, context.response(), it.cause())
        }
    }

    val handleGetAroPlans = Handler<RoutingContext> { context ->
        val db = context.get<SQLConnection>("dbconnection")
        val params = JsonArray().add(context.request().getParam("province_id").toInt())

        db.queryWithParams(InternalQueries.GET_ARO_PLANS, params) {
            if (it.succeeded())
                context.response().endWithJson(it.result().rows)
            else
                sendError(500, context.response(), it.cause())
        }

    }

    val handleDeleteAroPlan = Handler<RoutingContext> { context ->
        val db = context.get<SQLConnection>("dbconnection")
        val params = JsonArray()

        val id = context.request().formAttributes().get("id")

        params.add(id.toInt())

        db.updateWithParams(InternalQueries.DELETE_ARO_PLAN, params) {
            if (it.succeeded())
                context.response().endWithJson(JsonObject().put("status", "success"))
            else
                sendError(500, context.response(), it.cause())
        }
    }
}