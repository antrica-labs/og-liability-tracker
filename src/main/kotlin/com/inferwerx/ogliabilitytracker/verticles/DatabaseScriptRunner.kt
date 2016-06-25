package com.inferwerx.ogliabilitytracker.verticles

import io.vertx.core.AbstractVerticle
import io.vertx.core.json.JsonObject
import io.vertx.ext.jdbc.JDBCClient
import java.nio.charset.Charset
import java.nio.file.Files
import java.nio.file.Paths
import java.sql.Connection
import java.sql.DriverManager
import java.sql.Statement

/**
 * This verticle is used to run SQL scripts from files. It useful for creating database objects and running tasks.
 *
 * In order to get this verticle to execute a script, you only need to put the full path to the script file that you wish
 * to execute onto the event bus, addressed to 'og-liability-tracker.db_script_runner'.
 */
class DatabaseScriptRunner : AbstractVerticle() {
    override fun start() {
        vertx.eventBus().consumer<String>("og-liability-tracker.db_script_runner") { message ->
            val job = JsonObject(message.body())
            val scripts = job.getJsonArray("scripts")
            var counter = 0

            vertx.executeBlocking<Void>({ future ->
                Class.forName(config().getString("db.jdbc_driver"))

                var connection : Connection? = null
                var statement : Statement? = null

                try {
                    connection = DriverManager.getConnection("${config().getString("db.url_proto")}${config().getString("db.file_path")}${config().getString("db.url_options")}", config().getString("db.username"), config().getString("db.password"))
                    statement = connection.createStatement()

                    for (script in scripts) {
                        val content = readFile(script.toString(), Charset.defaultCharset())

                        statement.execute(content)

                        counter++
                    }

                    future.complete()
                } catch (t : Throwable) {
                    future.fail(t)

                    connection?.close()
                }
            }, {
                if (it.succeeded())
                    message.reply(JsonObject().put("message", "Executed ${counter} scripts").put("counter", counter).encode())
                else
                    message.fail(1, it.cause().toString())
            })
        }
    }



    /**
     * Reads a file into a string. The file should be relatively small unless you want to use a lot of memory space...
     */
    private fun readFile(path : String, encoding: Charset) : String {
        val bytes = Files.readAllBytes(Paths.get(path));

        return String(bytes, encoding)
    }
}