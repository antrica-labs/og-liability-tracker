package com.inferwerx.ogliabilitytracker.verticles.workers

import io.vertx.core.AbstractVerticle
import io.vertx.core.json.JsonObject


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

            message.fail(1, "Not yet implemented")
        }
    }
}