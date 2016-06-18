package com.inferwerx.ogliabilitytracker.verticles

import io.vertx.core.AbstractVerticle
import io.vertx.core.json.JsonObject
import java.io.File

/**
 * This worker verticle handles the processing of LLR files that are downloaded from DDS.
 */
class AlbertaLiabilityImporter : AbstractVerticle() {
    override fun start() {
        vertx.eventBus().consumer<String>("og-liability-tracker.ab_importer") { message ->
            val file = JsonObject(message.body())

            val path = "${System.getProperty("user.dir")}${File.separator}${file.getString("uploadedFileName")}"

            // TODO: parse and return the details of this file
        }
    }
}