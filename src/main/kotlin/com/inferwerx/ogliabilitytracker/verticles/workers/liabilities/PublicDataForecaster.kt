package com.inferwerx.ogliabilitytracker.verticles.workers.liabilities

import io.vertx.core.AbstractVerticle
import io.vertx.core.eventbus.DeliveryOptions
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject

/**
 * Forecasts 6 months of LMR ratings from list of well and facility licences. The liabilities of each licence must be
 * included as well, as these are difficult to forecast without additional information
 */
class PublicDataForecaster : AbstractVerticle() {
    override fun start() {
        vertx.eventBus().consumer<String>("og-liability-tracker.public_data_forecaster") { message ->
            try {
                val job = JsonObject(message.body())

                val netbacks = job.getJsonArray("netbacks")
                val licences = job.getJsonArray("licences")

                val historical = createHistoricalLmr(licences, netbacks)

                val forward = JsonObject()

                forward.put("netbacks", netbacks)
                forward.put("historical_lmr", historical)

                vertx.eventBus().send<String>("og-liability-tracker.simple_forecaster", forward.encode()) { reply ->
                    if (reply.succeeded()) {
                        message.reply(reply.result().body())
                    } else {
                        message.fail(1, reply.cause().message)
                    }
                }
            } catch (t : Throwable) {
                message.fail(1, t.message)
            }
        }
    }

    private fun createHistoricalLmr(licences : JsonArray, netbacks : JsonArray) : JsonArray {
        val history = JsonArray()


        return history
    }
}

/*
SELECT
  pd.product_type,
  pd.year,
  sum(pd.jan_volume) AS jan_volume,
  sum(pd.feb_volume) AS feb_volume,
  sum(pd.mar_volume) AS mar_volume,
  sum(pd.apr_volume) AS apr_volume,
  sum(pd.may_volume) AS may_volume,
  sum(pd.jun_volume) AS jun_volume,
  sum(pd.jul_volume) AS jul_volume,
  sum(pd.aug_volume) AS aug_volume,
  sum(pd.sep_volume) AS sep_volume,
  sum(pd.oct_volume) AS oct_volume,
  sum(pd.nov_volume) AS nov_volume,
  sum(pd.dec_volume) AS dec_volume
FROM PDEN_VOL_BY_MONTH pd INNER JOIN well_license wl ON pd.pden_id = wl.uwi
WHERE pd.product_type IN ('P-OIL', 'P-GAS') AND pd.year >= 2015 AND wl.license_id = 'AB0139408'
GROUP BY pd.PRODUCT_TYPE, pd.year
ORDER BY pd.year desc;
 */