package com.inferwerx.ogliabilitytracker.verticles.workers.liabilities

import io.vertx.core.AbstractVerticle
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import java.time.Instant
import java.util.*


/**
 * This worker verticle is used for calculating future LMR ratings for a given set of licences or from historical LMR ratings.
 * The call to the event bus should contain a json object with an attribute called 'netbacks' containing a list of historical
 * netbacks, and one of either of the following data sets:
 *
 *    'historical_lmr' - a list of asset and liability values by month
 *    'licences' - a list of well and facility licence numbers and a date range for pulling historical production data
 *
 * If licences are passed in, the forecaster will attempt to look up historical production data in IHS to calculate historical
 * asset values, and use location and well depth to try and estimate historical liability values. The liability estimate
 * is unlikely to be very accurate as it's impossible to know if there are any site specific, vent flow, or other added
 * liabilities.
 */
class LiabilityForecaster : AbstractVerticle() {
    override fun start() {
        vertx.eventBus().consumer<String>("og-liability-tracker.forecaster") { message ->
            val job = JsonObject(message.body())

            val netbacks = job.getJsonArray("netbacks")
            val history = job.getJsonArray("historical_lmr")

            val lastLmr = history.getJsonObject(history.size() - 1)
            val liabilityValue = lastLmr.getDouble("liability_value")

            val monthlyVolumes = calculateMonthlyVolumes(history, netbacks)


            message.fail(1, "Not yet implemented")
        }
    }

    private fun calculateMonthlyVolumes(history : JsonArray, netbacks : JsonArray) : Array<Double> {
        val list = LinkedList<Double>()
        val netbackDates = ArrayList<Instant>(netbacks.size())
        val netbackLookup = HashMap<Instant, Double>()

        for (item in netbacks) {
            val netback = item as JsonObject

            netbackDates.add(netback.getInstant("effecitve_date"))
            netbackLookup.put(netback.getInstant("effective_date"), netback.getDouble("netback"))
        }

        for (item in history) {
            val report = item as JsonObject

            val reportDate = report.getInstant("report_date")

            var matchingNetbackDate : Instant? = null
            for (i in 0..netbackDates.size) {
                if (reportDate > netbackDates[i]) {
                    matchingNetbackDate = netbackDates[i]

                    break
                }
            }

            if (matchingNetbackDate != null) {
                val netback = netbackLookup[matchingNetbackDate]
                var volume = report.getDouble("asset_value") / netback!!


            }
        }


        return list.toArray() as Array<Double>
    }
}