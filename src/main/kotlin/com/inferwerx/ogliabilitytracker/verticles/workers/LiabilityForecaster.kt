package com.inferwerx.ogliabilitytracker.verticles.workers

import io.vertx.core.AbstractVerticle
import io.vertx.core.json.JsonObject


class LiabilityForecaster : AbstractVerticle() {
    override fun start() {
        vertx.eventBus().consumer<String>("og-liability-tracker.forecaster") { message ->
            val job = JsonObject(message.body())

            message.fail(1, "Not yet implemented")
        }
    }
}