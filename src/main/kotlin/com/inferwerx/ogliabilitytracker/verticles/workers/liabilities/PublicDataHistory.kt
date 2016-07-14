package com.inferwerx.ogliabilitytracker.verticles.workers.liabilities

import com.inferwerx.ogliabilitytracker.queries.PublicDataQueries
import io.vertx.core.AbstractVerticle
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.ext.jdbc.JDBCClient
import java.sql.Connection
import java.sql.DriverManager
import java.time.*
import java.time.temporal.TemporalAdjusters;
import java.util.*

/**
 * Forecasts 6 months of LMR ratings from list of well and facility licences. The liabilities of each licence must be
 * included as well, as these are difficult to forecast without additional information
 */
class PublicDataHistory : AbstractVerticle() {
    override fun start() {
        vertx.eventBus().consumer<String>("og-liability-tracker.public_data_history") { message ->
            vertx.executeBlocking<JsonArray>({ future ->
                var connection: Connection? = null

                try {
                    val job = JsonObject(message.body())
                    val netbacks = job.getJsonArray("netbacks")
                    val licences = job.getJsonArray("licences")

                    Class.forName(config().getString("db.jdbc_driver"))

                    connection = DriverManager.getConnection(config().getString("db.public.url"), config().getString("db.public.username"), config().getString("db.public.password"))

                    val historical = createHistoricalLmr(connection, licences, netbacks)

                    future.complete(historical)
                } catch (t: Throwable) {
                    connection?.close()

                    future.fail(t)
                }
            }, {
                if (it.succeeded())
                    message.reply(it.result().encode())
                else
                    message.fail(1, it.cause().toString())
            })
        }
    }

    private fun createHistoricalLmr(connection : Connection, licences : JsonArray, netbacks : JsonArray) : JsonArray {
        val history = JsonArray()
        val queryStartDate = LocalDate.now().with(TemporalAdjusters.firstDayOfMonth()).minusMonths(26)
        val paramListBuilder = StringBuilder()
        val volumes = HashMap<Int, ArrayList<MonthlyVolume>>()
        var packageLiability = 0.0
        val netbackDates = ArrayList<Instant>(netbacks.size())
        val netbackLookup = HashMap<Instant, JsonObject>()

        // Setup a lookup for quickly finding netbacks given a specific date
        for (item in netbacks) {
            val netback = item as JsonObject

            netbackDates.add(netback.getInstant("effective_date"))
            netbackLookup.put(netback.getInstant("effective_date"), netback)
        }

        // Make sure the dates are in reverse order
        netbackDates.sortDescending()


        // We combine the volumes of all of the licences, so we have to build the IN clause parameters based on the
        // number of licences. This could result in problems if the licence list is too large.
        for (i in 0..licences.size() - 1) {
            paramListBuilder.append("?,")
        }

        val query = PublicDataQueries.GET_HISTORICAL_VOLUMES_BY_LICENCE.replace("%%IN_CLAUSE%%", paramListBuilder.deleteCharAt(paramListBuilder.length - 1).toString())
        val statement = connection.prepareStatement(query)

        statement.setInt(1, queryStartDate.year)

        for (i in 0..licences.size() - 1) {
            val licence = licences.getValue(i) as JsonObject

            statement.setString(i + 2, "${licence.getString("province_short_name")}${String.format("%07d", licence.getInteger("licence"))}")
            packageLiability += licence.getDouble("liability_amount")
        }

        val rs = statement.executeQuery()

        // Build up the monthly volumes structure from the query result set. This set will be use to calculate asset values.
        while (rs.next()) {
            val months = ArrayList<MonthlyVolume>()

            var count = 2
            while (count < 26) {
                val month = MonthlyVolume()

                month.gas = rs.getDouble(count++)
                month.oil = rs.getDouble(count++)

                months.add(month)
            }

            volumes.put(rs.getInt(1), months)
        }

        val netbackScopeDate = LocalDate.now().atStartOfDay(ZoneId.systemDefault()).toInstant()
        var matchingNetbackDate : Instant? = null
        for (k in 0..netbackDates.size - 1) {
            if (netbackScopeDate >= netbackDates[k]) {
                matchingNetbackDate = netbackDates[k]

                break
            }
        }

        if (matchingNetbackDate != null) {
            val netback = netbackLookup.get(matchingNetbackDate)!!
            val shrinkage = netback.getDouble("shrinkage_factor")
            val conversion = netback.getDouble("oil_equivalent_conversion")

            // Now calculate the current asset value
            val calcStartDate = queryStartDate.plusMonths(14)
            for (i in 0..12) {
                val workingMonth = calcStartDate.plusMonths(i.toLong())
                val workingDate = workingMonth.atStartOfDay(ZoneId.systemDefault()).toInstant()

                var assetValue = 0.0

                // For each month, sum up the revenue from 14 months earlier to 2 months earlier, multiplied by 3
                for (j in 2..13) {
                    val prodMonth = workingMonth.minusMonths(j.toLong())
                    val twelveMonths = volumes.get(prodMonth.year)

                    if (twelveMonths != null) {
                        val production = twelveMonths[prodMonth.monthValue - 1]

                        val monthValue = ((production.gas!! * shrinkage / conversion) + production.oil!!) * netback.getDouble("netback") * 3
                        assetValue += monthValue
                        assetValue += 0
                    }
                }

                val record = JsonObject()

                record.put("report_date", workingDate)
                record.put("asset_value", assetValue)
                record.put("liability_value", packageLiability)
                if (packageLiability == 0.0)
                    record.put("rating", 0.0)
                else
                    record.put("rating", assetValue / packageLiability)
                record.put("net_value", assetValue - packageLiability)

                history.add(record)
            }
        }

        return history
    }

    data class MonthlyVolume(var oil : Double? = null, var gas : Double? = null)
}