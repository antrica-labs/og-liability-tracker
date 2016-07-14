package com.inferwerx.ogliabilitytracker.verticles.workers.liabilities

import io.vertx.core.AbstractVerticle
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import java.time.*
import java.util.*


/**
 * This worker verticle is used for calculating future LMR ratings from historical LMR ratings.
 * The call to the event bus should contain a json object with an attribute called 'netbacks' containing a list of historical
 * netbacks, and 'historical_lmr' containing a list of asset and liability values by month.
 */
class HistoricalRatingsForecaster : AbstractVerticle() {
    override fun start() {
        vertx.eventBus().consumer<String>("og-liability-tracker.simple_forecaster") { message ->
            try {
                val job = JsonObject(message.body())

                val netbacks = job.getJsonArray("netbacks")
                val history = job.getJsonArray("historical_lmr")

                val results = forecastFromHistorical(history, netbacks)

                message.reply(results.encode())
            } catch (t : Throwable) {
                message.fail(1, t.message)
            }
        }
    }

    private fun forecastFromHistorical(history : JsonArray, netbacks : JsonArray) : JsonArray {
        val forecast = JsonArray()
        val lastLmr = history.getJsonObject(history.size() - 1)
        val constantLiabilityValue = lastLmr.getDouble("liability_value")

        val monthlyVolumes = calculateMonthlyVolumes(history, netbacks)
        val decline = calculateAverageDecline(monthlyVolumes)

        val lastMonth = LocalDateTime.ofInstant(lastLmr.getInstant("report_date"), ZoneId.of("Z")).withDayOfMonth(1)
        var previous = history.getValue(history.size() - 1) as JsonObject

        for (i in 1..6) {
            val report = JsonObject()

            report.put("report_date", lastMonth.plusMonths(i.toLong()).toInstant(ZoneOffset.UTC))
            report.put("asset_value", previous.getDouble("asset_value") * (1 + decline))
            report.put("liability_value", constantLiabilityValue)
            if (report.getDouble("liability_value") == 0.0)
                report.put("rating", 0.0)
            else
                report.put("rating", report.getDouble("asset_value") / report.getDouble("liability_value"))
            report.put("net_value", report.getDouble("asset_value") - report.getDouble("liability_value"))

            previous = report
            forecast.add(report)
        }

        return forecast
    }

    /**
     * Since the netback value changes from time to time, we need to convert asset values back to volumes using the
     * historical netbacks in order to trend the decline accurately.
     */
    private fun calculateMonthlyVolumes(history : JsonArray, netbacks : JsonArray) : ArrayList<Double> {
        val list = ArrayList<Double>(history.size())
        val netbackDates = ArrayList<Instant>(netbacks.size())
        val netbackLookup = HashMap<Instant, Double>()

        for (item in netbacks) {
            val netback = item as JsonObject

            netbackDates.add(netback.getInstant("effective_date"))
            netbackLookup.put(netback.getInstant("effective_date"), netback.getDouble("netback"))
        }

        // Make sure the dates are in reverse order
        netbackDates.sortDescending()

        for (item in history) {
            val report = item as JsonObject

            val reportDate = report.getInstant("report_date")

            var matchingNetbackDate : Instant? = null
            for (i in 0..netbackDates.size - 1) {
                if (reportDate >= netbackDates[i]) {
                    matchingNetbackDate = netbackDates[i]

                    break
                }
            }

            if (matchingNetbackDate != null)
                list.add(report.getDouble("asset_value") / netbackLookup[matchingNetbackDate]!!)
        }

        return list
    }

    private fun calculateAverageDecline(list : List<Double>) : Double {
        val declines = ArrayList<Double>(list.size - 1)

        for (i in 1..list.size - 1) {
            declines.add(list[i] / list[i - 1] - 1)
        }

        return declines.sum() / declines.size
    }
}