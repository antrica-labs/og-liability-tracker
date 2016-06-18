package com.inferwerx.ogliabilitytracker.verticles

import io.vertx.core.AbstractVerticle
import io.vertx.core.json.JsonObject
import io.vertx.ext.jdbc.JDBCClient

/**
 * This verticle is used to run SQL scripts from files. It useful for creating database objects and running tasks.
 *
 * In order to get this verticle to execute a script, you only need to put the full path to the script file that you wish
 * to execute onto the event bus, addressed to 'og-liability-tracker.db_script_runner'.
 */
class DatabaseScriptRunner : AbstractVerticle() {
    override fun start() {
        val dbConfig = JsonObject()
                .put("driver_class", config().getString("db.jdbc_driver"))
                .put("url", "${config().getString("db.url_proto")}${config().getString("db.file_path")}${config().getString("db.url_options")}")
                .put("user", config().getString("db.username"))
                .put("password", config().getString("db.password"))
        val dbClient = JDBCClient.createShared(vertx, dbConfig)

        // Read file names from the event bus
        vertx.eventBus().consumer<String>("og-liability-tracker.db_script_runner") { message ->
            var file = message.body()

            // Get the SQL script inside of the file
            vertx.fileSystem().readFile(file, { result ->
                if (result.failed()) {
                    message.reply(JsonObject().put("status", "failed").put("cause", result.cause().message).encode())

                    return@readFile
                } else {
                    var buffer = result.result()

                    dbClient.getConnection { connection ->
                        if (connection.failed()) {
                            message.reply(JsonObject().put("status", "failed").put("cause", connection.cause().message).encode())

                            return@getConnection
                        }

                        var db = connection.result()

                        // Execute the script as is... this is probably not the safest thing to do, but it's the simplest
                        db.execute(buffer.toString(), { query ->
                            if (query.failed()) {
                                message.reply(JsonObject().put("status", "failed").put("cause", query.cause().message).encode())
                            } else {
                                message.reply(JsonObject().put("status", "success").encode())
                            }

                            db.close()
                        })
                    }
                }
            })
        }

    }
}