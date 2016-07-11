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

/**
 * Creates an acquisition target from a CSV file containing a list of well and facility licences, along with their liability amounts
 */
class AcquisitionImporter : AbstractVerticle() {
    override fun start() {
        vertx.eventBus().consumer<String>("og-liability-tracker.acquisition_importer") { message ->
            val job = JsonObject(message.body())

            vertx.executeBlocking<Int>({ future ->
                try {
                    val importCount = importFile(job.getInteger("province_id"), job.getString("description"), job.getInstant("effective_date"), job.getDouble("purchase_price"), job.getString("filename"))

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

    private fun importFile(province : Int, description : String, effectiveDate : Instant, purchasePrice : Double, filename : String) : Int {
        Class.forName(config().getString("db.jdbc_driver"))

        val recordsPersisted : Int
        var connection : Connection? = null
        var parser : CSVParser? = null

        try {
            connection = DriverManager.getConnection("${config().getString("db.url_proto")}${config().getString("db.file_path")}${config().getString("db.url_options")}", config().getString("db.username"), config().getString("db.password"))
            parser = CSVFormat.EXCEL.withHeader(ImportHeaders::class.java).parse(FileReader(filename))

            val insertAcqStatement = connection.prepareStatement(InternalQueries.INSERT_ACQUISITION, Statement.RETURN_GENERATED_KEYS)
            val insertLicenceStatement = connection.prepareStatement(InternalQueries.INSERT_ACQUISITION_LICENCE)

            insertAcqStatement.setInt(1, province)
            insertAcqStatement.setBoolean(2, true)
            insertAcqStatement.setString(3, description)
            insertAcqStatement.setDate(4, java.sql.Date(java.util.Date.from(effectiveDate).time))
            insertAcqStatement.setDouble(5, purchasePrice)

            insertAcqStatement.executeUpdate()

            val rs = insertAcqStatement.generatedKeys
            val acq : Int

            if (rs.next()) {
                acq = rs.getInt(1)
            } else {
                throw Throwable("Unable to get acquisition ID")
            }

            for (record in parser) {
                // skip the header row
                if (record.get(ImportHeaders.Type) == "Type" && record.get(ImportHeaders.Licence) == "Licence")
                    continue

                insertLicenceStatement.setInt(1, acq)
                insertLicenceStatement.setString(2, record.get(ImportHeaders.Type))
                insertLicenceStatement.setString(3, record.get(ImportHeaders.Licence))
                insertLicenceStatement.setDouble(4, record.get(ImportHeaders.LiabilityAmount).toDouble())

                insertLicenceStatement.addBatch()
            }

            connection.autoCommit = false

            recordsPersisted = insertLicenceStatement.executeBatch().size

            connection.commit()

            insertAcqStatement.close()
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
        Licence,
        LiabilityAmount
    }
}