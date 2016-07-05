package com.inferwerx.ogliabilitytracker.verticles.workers

import io.vertx.core.AbstractVerticle
import io.vertx.core.json.JsonObject
import org.jxls.util.JxlsHelper
import org.jxls.common.Context
import java.io.FileInputStream
import java.io.FileOutputStream
import java.io.InputStream
import java.io.OutputStream
import java.sql.Connection
import java.sql.DriverManager
import java.text.SimpleDateFormat
import java.util.*

class LiabilityExporter : AbstractVerticle() {
    private val provinceQuery = "SELECT name, short_name FROM provinces WHERE province_id = ?"
    private val companyQuery = "SELECT name, alt_name FROM companies WHERE company_id = ?"
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

                    val outputFile = "${company.altName}-${province.shortName}-${job.getString("report_date")}"

                    writeToFile(outputFile, job.getString("filename"), job.getString("report_date"), province, company, liabilities)
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

    private fun getCompany(connection : Connection, id : Int) : Company {
        val statement = connection.prepareStatement(companyQuery)

        statement.setInt(1, id)
        statement.addBatch()

        val rs = statement.executeQuery()

        if (!rs.next())
            throw Throwable("Unable to find company with id = ${id}")

        return Company(
                name = rs.getString(0),
                altName = rs.getString(1)
        )
    }

    private fun getProvince(connection : Connection, id : Int) : Province {
        val statement = connection.prepareStatement(provinceQuery)

        statement.setInt(1, id)
        statement.addBatch()

        val rs = statement.executeQuery()

        if (!rs.next())
            throw Throwable("Unable to find province with id = ${id}")

        return Province(
                name = rs.getString(0),
                shortName = rs.getString(1)
        )
    }

    private fun getLiabilities(connection : Connection, companyId : Int, provinceId: Int, reportDateStr : String) : List<ExportRecord> {
        val list = LinkedList<ExportRecord>()
        val statement = connection.prepareStatement(liabilityDetailsQuery)

        val dateFormat = SimpleDateFormat("yyyy-MM-dd", Locale.CANADA)
        val reportDate = dateFormat.parse(reportDateStr)

        statement.setLong(1, reportDate.time / 1000) // Todo: replace with actual time from reportDateStr
        statement.setInt(2, companyId)
        statement.setInt(3, provinceId)

        val rs = statement.executeQuery()

        while (rs.next()) {
            val record = ExportRecord(
                    type = rs.getString(1),
                    licence = rs.getString(2),
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
        val context = Context()
        var input : InputStream? = null
        var output : OutputStream? = null

        try {
            input = FileInputStream(templateFile)
            output = FileOutputStream(outputFile)

            context.putVar("reportDate", reportDate)
            context.putVar("province", province)
            context.putVar("company", company)
            context.putVar("records", records)

            JxlsHelper.getInstance().processTemplate(input, output, context)

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
            val hierarchyElement : String,
            val type : String,
            val licence : String,
            val status : String,
            val location : String,
            val assetValue : Double,
            val liabilityValue : Double,
            val psv : String,
            val abandonmentBasic : Double,
            val abandonmentAdditionalEvent : Double,
            val abandonmentGwp : Double,
            val abandonmentGasMigration : Double,
            val abandonmentVentFlow : Double,
            val abandonmentSiteSpecific : Double,
            val reclamationBasic : Double,
            val reclamationSiteSpecific : Double
    )
}