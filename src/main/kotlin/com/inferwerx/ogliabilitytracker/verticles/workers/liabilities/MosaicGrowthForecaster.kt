package com.inferwerx.ogliabilitytracker.verticles.workers.liabilities

import com.inferwerx.ogliabilitytracker.queries.MosaicQueries
import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.ext.jdbc.JDBCClient
import java.time.Instant
import java.time.ZoneId

class MosaicGrowthForecaster : AbstractVerticle() {
    override fun start() {
        val dbConfig = JsonObject()
                .put("driver_class", config().getString("db.mosaic.jdbc_driver"))
                .put("url", config().getString("db.mosaic.url"))
                .put("user", config().getString("db.mosaic.username"))
                .put("password", config().getString("db.mosaic.password"))
        val dbClient = JDBCClient.createNonShared(vertx, dbConfig)

        vertx.eventBus().consumer<String>("og-liability-tracker.mosaic_forecaster") { message ->
            val job = JsonObject(message.body())
            val replyFuture = Future.future<JsonObject>()

            when (job.getString("request")) {
                "entity-list" -> getEntityList(job.getInstant("start_date"), dbClient, replyFuture)
                "forecast" -> getEntityForecast(job.getInstant("start_date"), job.getJsonObject("netback"), dbClient, replyFuture)
            }

            replyFuture.setHandler {
                if (it.succeeded())
                    message.reply(it.result().encode())
                else
                    message.fail(1, it.cause().message)
            }
        }
    }

    private fun getEntityList(startDate : Instant, dbClient : JDBCClient, future : Future<JsonObject>) {
        dbClient.getConnection {
            if (it.failed()) {
                future.fail(it.cause())
            } else {
                val connection = it.result()

                connection.queryWithParams(MosaicQueries.GET_ENTITY_LIST, JsonArray().add(startDate).add(startDate)) { query ->
                    if (query.failed()) {
                        future.fail(query.cause())
                    } else {
                        future.complete(JsonObject().put("entities", JsonArray(query.result().rows)))
                    }
                }
            }
        }
    }

    private fun getEntityForecast(startDate : Instant, netback : JsonObject, dbClient: JDBCClient, future: Future<JsonObject>) {
        dbClient.getConnection {
            if (it.failed()) {
                future.fail(it.cause())
            } else {
                val connection = it.result()
                val volumeFuture = Future.future<JsonArray>()

                // First get the future volumes and asset values from the database
                connection.queryWithParams(MosaicQueries.GET_GROWTH_PRODUCTION, JsonArray().add(startDate).add(startDate)) { query ->
                    if (query.failed()) {
                        volumeFuture.fail(query.cause())
                    } else {
                        val forecasts = JsonArray()
                        var currentEntity : JsonObject? = null

                        // The shrinkage conversion isn't going to be used (for now) as Mosaic gives us a sales gas number,
                        // so raw gas would have to be back calculated first which isn't that easy. This will mean that
                        // the asset value generated here won't match perfectly, but it should be close enough.
                        val conversion = netback.getDouble("oil_equivalent_conversion")
                        val netbackValue = netback.getDouble("netback")

                        for (row in query.result().rows) {
                            if (currentEntity == null || currentEntity.getString("entity") != row.getString("entity_name")) {
                                currentEntity = JsonObject().put("entity", row.getString("entity_name")).put("forecast", JsonArray())

                                forecasts.add(currentEntity)
                            }

                            val month = JsonObject()

                            val gas = row.getDouble("gas_volume")
                            val oil = row.getDouble("oil_volume")
                            val gasOilEq = gas / conversion

                            month.put("production_month", row.getInstant("production_month"))
                            month.put("gas_volume", gas)
                            month.put("oil_volume", oil)
                            month.put("revenue", (gasOilEq + oil) * netbackValue * 3)

                            currentEntity!!.getJsonArray("forecast").add(month)
                        }

                        volumeFuture.complete(forecasts)
                    }
                }

                // Calculate the future LMR asset values
                volumeFuture.setHandler {
                    if (it.failed()) {
                        future.fail(it.cause())
                    } else {
                        val forecasts = JsonArray()

                        for (obj in it.result()) {
                            val revenueEntity = obj as JsonObject

                            val entity = JsonObject().put("entity", revenueEntity.getString("entity")).put("forecast", JsonArray())

                            var assetValue = 0.0
                            for (obj2 in revenueEntity.getJsonArray("forecast")) {
                                val record = obj2 as JsonObject

                                assetValue += record.getDouble("revenue")

                                val month = JsonObject()

                                val futureReportDate = record.getInstant("production_month").atZone(ZoneId.systemDefault()).plusMonths(2).toInstant()

                                month.put("report_date", futureReportDate)
                                month.put("asset_value", assetValue)
                                month.put("liability_value", 0.0)
                                month.put("rating", 0)
                                month.put("net_value", month.getDouble("asset_value") - month.getDouble("liability_value"))

                                entity.getJsonArray("forecast").add(month)



                            }

                            forecasts.add(entity)
                        }

                        future.complete(JsonObject().put("forecasts", forecasts))
                    }

                    connection.close()
                }

            }
        }
    }
}
