package com.inferwerx.ogliabilitytracker.verticles.workers.liabilities

import com.inferwerx.ogliabilitytracker.queries.InternalQueries
import io.vertx.core.AbstractVerticle
import io.vertx.core.json.JsonObject
import org.apache.poi.util.Internal
import org.jxls.common.Context
import org.jxls.util.JxlsHelper
import java.io.*
import java.sql.Connection
import java.sql.DriverManager
import java.text.SimpleDateFormat
import java.util.*

class DetailedReportExporter : AbstractVerticle() {
    override fun start() {
        val workingDir = config().getString("working-dir")

        vertx.eventBus().consumer<String>("og-liability-tracker.liability_exporter") { message ->
            val job = JsonObject(message.body())

            vertx.executeBlocking<String>({ future ->
                var connection : Connection? = null

                try {
                    Class.forName(config().getString("db.jdbc_driver"))

                    connection = DriverManager.getConnection("${config().getString("db.url_proto")}${config().getString("db.file_path")}${config().getString("db.url_options")}", config().getString("db.username"), config().getString("db.password"))

                    val province = getProvince(connection, job.getInteger("province"))
                    val liabilities = getLiabilities(connection, job.getInteger("province"), job.getString("report_date"))

                    val originalFilename = job.getString("originalFilename")
                    val outputFile = "${workingDir}${File.separator}${province.shortName}-${job.getString("report_date")}${getFileExtension(originalFilename)}"

                    writeToFile(outputFile, job.getString("filename"), job.getString("report_date"), province, liabilities)

                    future.complete(outputFile)
                } catch (t : Throwable) {
                    connection?.close()

                    future.fail(t)
                }
            }, {
                if (it.succeeded())
                    message.reply(it.result())
                else
                    message.fail(1, it.cause().toString())
            })

        }
    }

    private fun getFileExtension(filename : String) : String {
        var extension = ""

        val i = filename.lastIndexOf('.')
        val p = Math.max(filename.lastIndexOf('/'), filename.lastIndexOf('\\'))

        if (i > p) {
            extension = ".${filename.substring(i + 1)}"
        }

        return extension
    }

    private fun getProvince(connection : Connection, id : Int) : Province {
        val statement = connection.prepareStatement(InternalQueries.GET_PROVINCE_BY_ID)

        statement.setInt(1, id)

        val rs = statement.executeQuery()

        if (!rs.next())
            throw Throwable("Unable to find province with id = ${id}")

        return Province(
                name = rs.getString(1),
                shortName = rs.getString(2)
        )
    }

    private fun getLiabilities(connection : Connection, provinceId: Int, reportDateStr : String) : List<ExportRecord> {
        val list = LinkedList<ExportRecord>()
        val statement = connection.prepareStatement(InternalQueries.GET_REPORT_DETAILS)

        val dateFormat = SimpleDateFormat("yyyy-MM-dd", Locale.CANADA)
        val reportDate = java.sql.Date(dateFormat.parse(reportDateStr).time)

        statement.setDate(1, reportDate)
        statement.setInt(2, provinceId)

        val rs = statement.executeQuery()

        while (rs.next()) {
            val record = ExportRecord(
                    type = rs.getString(1),
                    licence = rs.getInt(2),
                    location = rs.getString(3),
                    hierarchyElement = rs.getString(4),
                    status = rs.getString(5),
                    psv = rs.getString(6),
                    assetValue = rs.getDouble(7),
                    liabilityValue = rs.getDouble(8),
                    abandonmentBasic = rs.getDouble(9),
                    abandonmentAdditionalEvent = rs.getDouble(10),
                    abandonmentGwp = rs.getDouble(11),
                    abandonmentGasMigration = rs.getDouble(12),
                    abandonmentVentFlow = rs.getDouble(13),
                    abandonmentSiteSpecific = rs.getDouble(14),
                    reclamationBasic = rs.getDouble(15),
                    reclamationSiteSpecific = rs.getDouble(16)
            )

            list.add(record)
        }

        if (list.count() == 0)
            throw Throwable("Unable to find liabilities with province_id = ${provinceId}, report_date = ${reportDateStr}")

        return list
    }

    private fun writeToFile(outputFile : String, templateFile : String, reportDate : String, province : Province, records : List<ExportRecord>) {
        var input : InputStream? = null
        var output : OutputStream? = null

        try {
            input = FileInputStream(templateFile)
            output = FileOutputStream(outputFile)

            val context = Context()

            context.putVar("reportDate", reportDate)
            context.putVar("province", province)
            context.putVar("records", records)

            val helper = JxlsHelper.getInstance()

            helper.processTemplate(input, output, context)

            input.close()
            output.close()
        } finally {
            input?.close()
            output?.close()
        }
    }

    data class Province (
            val name : String,
            val shortName : String
    )

    data class ExportRecord (
            val hierarchyElement : String?,
            val type : String,
            val licence : Int,
            val status : String,
            val location : String,
            val assetValue : Double,
            val liabilityValue : Double,
            val psv : String,
            val abandonmentBasic : Double,
            val abandonmentAdditionalEvent : Double,
            val abandonmentGwp : Double,
            val abandonmentVentFlow : Double,
            val abandonmentGasMigration : Double,
            val abandonmentSiteSpecific : Double,
            val reclamationBasic : Double,
            val reclamationSiteSpecific : Double
    )
}