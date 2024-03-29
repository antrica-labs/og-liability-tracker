package com.inferwerx.ogliabilitytracker.verticles.workers.importers

import com.inferwerx.ogliabilitytracker.queries.InternalQueries
import io.vertx.core.AbstractVerticle
import io.vertx.core.json.JsonObject
import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.CSVParser
import java.io.FileReader
import java.sql.Connection
import java.sql.DriverManager

class HierarchyImporter : AbstractVerticle() {
    override fun start() {
        vertx.eventBus().consumer<String>("og-liability-tracker.hierarchy_importer") { message ->
            val job = JsonObject(message.body())

            vertx.executeBlocking<Int>({ future ->
                try {
                    val importCount = importFile(job.getString("filename"))

                    future.complete(importCount)
                } catch (t : Throwable) {
                    future.fail(t)
                }
            }, {
                if (it.succeeded())
                    message.reply(JsonObject().put("message", "Saved ${it.result()} mappings").put("record_count", it.result()).encode())
                else
                    message.fail(1, it.cause().toString())
            })
        }
    }

    /**
     * Takes a CSV file in the format shown in webroot/resources/sample-hierarchy-update.csv and saves it to the database
     */
    private fun importFile(filename : String) : Int {
        Class.forName(config().getString("db.jdbc_driver"))

        val recordsPersisted : Int
        var connection : Connection? = null
        var parser : CSVParser? = null

        try {
            connection = DriverManager.getConnection("${config().getString("db.url_proto")}${config().getString("db.file_path")}${config().getString("db.url_options")}", config().getString("db.username"), config().getString("db.password"))
            parser = CSVFormat.EXCEL.withHeader(ImportHeaders::class.java).parse(FileReader(filename))

            val deleteStatement = connection.prepareStatement(InternalQueries.DELETE_HIERARCHY_LOOKUPS)
            val selectStatement = connection.prepareStatement(InternalQueries.INSERT_HIERARCHY_LOOKUP)

            for (record in parser) {
                // skip the header row
                if (record.get(ImportHeaders.Type) == "Type" && record.get(ImportHeaders.Licence) == "Licence" && record.get(ImportHeaders.HierarchyElement) == "HierarchyElement")
                    continue

                selectStatement.setString(1, record.get(ImportHeaders.Type))
                selectStatement.setInt(2, record.get(ImportHeaders.Licence).toInt())
                selectStatement.setString(3, record.get(ImportHeaders.HierarchyElement))

                selectStatement.addBatch()
            }

            connection.autoCommit = false

            deleteStatement.execute()

            recordsPersisted = selectStatement.executeBatch().size

            connection.commit()

            deleteStatement.close()
            selectStatement.close()

            parser.close()
            connection.close()

        } catch (t : Throwable) {
            connection?.close()
            parser?.close()

            throw t
        }

        return recordsPersisted
    }

    enum class ImportHeaders {
        Type,
        Licence,
        HierarchyElement
    }
}

