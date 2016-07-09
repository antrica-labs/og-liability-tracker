package com.inferwerx.ogliabilitytracker.verticles.workers.importers

import com.inferwerx.ogliabilitytracker.queries.InternalQueries
import io.vertx.core.AbstractVerticle
import io.vertx.core.json.JsonObject
import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.CSVParser
import java.io.FileReader
import java.sql.Connection
import java.sql.DriverManager
import java.sql.Statement
import java.time.Instant

class DispositionImporter : AbstractVerticle() {
    override fun start() {
        vertx.eventBus().consumer<String>("og-liability-tracker.disposition_importer") { message ->
            val job = JsonObject(message.body())

            vertx.executeBlocking<Int>({ future ->
                try {
                    val importCount = importFile(job.getString("description"), job.getInstant("effective_date"), job.getDouble("sale_price"), job.getString("filename"))

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
     * Takes a CSV file in the format shown in webroot/resources/sample-disposition-list.csv and saves it to the database
     */
    private fun importFile(description : String, effectiveDate : Instant, salePrice : Double, filename : String) : Int {
        Class.forName(config().getString("db.jdbc_driver"))

        val recordsPersisted : Int
        var connection : Connection? = null
        var parser : CSVParser? = null

        try {
            connection = DriverManager.getConnection("${config().getString("db.url_proto")}${config().getString("db.file_path")}${config().getString("db.url_options")}", config().getString("db.username"), config().getString("db.password"))
            parser = CSVFormat.EXCEL.withHeader(ImportHeaders::class.java).parse(FileReader(filename))

            val insertDispStatement = connection.prepareStatement(InternalQueries.INSERT_DISPOSITION, Statement.RETURN_GENERATED_KEYS)
            val insertLicenceStatement = connection.prepareStatement(InternalQueries.INSERT_DISPOSITION_ENTITY)

            insertDispStatement.setBoolean(1, true)
            insertDispStatement.setString(2, description)
            insertDispStatement.setDate(3, java.sql.Date(java.util.Date.from(effectiveDate).time))
            insertDispStatement.setDouble(4, salePrice)

            insertDispStatement.executeUpdate()

            val rs = insertDispStatement.generatedKeys
            var dispId = 0

            if (rs.next()) {
                dispId = rs.getInt(1)
            } else {
                throw Throwable("Unable to get disposition ID")
            }

            for (record in parser) {
                // skip the header row
                if (record.get(HierarchyImporter.ImportHeaders.Type) == "Type" && record.get(HierarchyImporter.ImportHeaders.Licence) == "Licence" && record.get(HierarchyImporter.ImportHeaders.HierarchyElement) == "HierarchyElement")
                    continue

                insertLicenceStatement.setInt(1, dispId)
                insertLicenceStatement.setString(2, record.get(ImportHeaders.Type))
                insertLicenceStatement.setString(3, record.get(ImportHeaders.Licence))

                insertLicenceStatement.addBatch()
            }

            connection.autoCommit = false

            recordsPersisted = insertLicenceStatement.executeBatch().size

            connection.commit()

            insertDispStatement.close()
            insertLicenceStatement.close()

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
        Licence
    }
}