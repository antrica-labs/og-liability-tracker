package com.inferwerx.ogliabilitytracker.verticles.workers.liabilities

import io.vertx.core.AbstractVerticle
import io.vertx.core.json.JsonObject
import org.jxls.common.Context
import org.jxls.util.JxlsHelper
import java.io.*
import java.sql.Connection
import java.sql.DriverManager
import java.text.SimpleDateFormat
import java.util.*

class LiabilityExporter : AbstractVerticle() {
    private val provinceQuery = "SELECT name, short_name FROM provinces WHERE id = ?"
    private val companyQuery = "SELECT name, alt_name FROM companies WHERE id = ?"
    private val liabilityDetailsQuery = """
        SELECT
          e.type,
          e.licence,
          e.location_identifier,
          h.hierarchy_value,
          r.entity_status,
          r.pvs_value_type,
          r.asset_value,
          r.liability_value,
          r.abandonment_basic,
          r.abandonment_additional_event,
          r.abandonment_gwp,
          r.abandonment_gas_migration,
          r.abandonment_vent_flow,
          r.abandonment_site_specific,
          r.reclamation_basic,
          r.reclamation_site_specific
        FROM entity_ratings r INNER JOIN entities e ON e.id = r.entity_id
          LEFT OUTER JOIN hierarchy_lookup h ON h.type = e.type AND h.licence = e.licence AND h.company_id = e.company_id
        WHERE r.report_month = ?
              AND e.province_id = ?
              AND e.company_id = ?
        ORDER BY h.hierarchy_value, e.licence
    """

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
                    val company = getCompany(connection, job.getInteger("company"))
                    val liabilities = getLiabilities(connection, job.getInteger("province"), job.getInteger("company"), job.getString("report_date"))

                    val originalFilename = job.getString("originalFilename")
                    val outputFile = "${workingDir}${File.separator}${company.altName}-${province.shortName}-${job.getString("report_date")}${getFileExtension(originalFilename)}"

                    writeToFile(outputFile, job.getString("filename"), job.getString("report_date"), province, company, liabilities)

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

    private fun getCompany(connection : Connection, id : Int) : Company {
        val statement = connection.prepareStatement(companyQuery)

        statement.setInt(1, id)

        val rs = statement.executeQuery()

        if (!rs.next())
            throw Throwable("Unable to find company with id = ${id}")

        return Company(
                name = rs.getString(1),
                altName = rs.getString(2)
        )
    }

    private fun getProvince(connection : Connection, id : Int) : Province {
        val statement = connection.prepareStatement(provinceQuery)

        statement.setInt(1, id)

        val rs = statement.executeQuery()

        if (!rs.next())
            throw Throwable("Unable to find province with id = ${id}")

        return Province(
                name = rs.getString(1),
                shortName = rs.getString(2)
        )
    }

    private fun getLiabilities(connection : Connection, companyId : Int, provinceId: Int, reportDateStr : String) : List<ExportRecord> {
        val list = LinkedList<ExportRecord>()
        val statement = connection.prepareStatement(liabilityDetailsQuery)

        val dateFormat = SimpleDateFormat("yyyy-MM-dd", Locale.CANADA)
        val reportDate = java.sql.Date(dateFormat.parse(reportDateStr).time)

        statement.setDate(1, reportDate)
        statement.setInt(2, companyId)
        statement.setInt(3, provinceId)

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
            throw Throwable("Unable to find liabilities with province_id = ${provinceId}, company_id = ${companyId}, report_month = ${reportDateStr}")

        return list
    }

    private fun writeToFile(outputFile : String, templateFile : String, reportDate : String, province : Province, company : Company, records : List<ExportRecord>) {
        var input : InputStream? = null
        var output : OutputStream? = null

        try {
            input = FileInputStream(templateFile)
            output = FileOutputStream(outputFile)

            val context = Context()

            context.putVar("reportDate", reportDate)
            context.putVar("province", province)
            context.putVar("company", company)
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

    data class Company (
            val name : String,
            val altName : String
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