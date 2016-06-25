package com.inferwerx.ogliabilitytracker.verticles

import com.inferwerx.ogliabilitytracker.alberta.AbLiability
import io.vertx.core.AbstractVerticle
import io.vertx.core.json.JsonObject
import java.io.File
import java.nio.charset.Charset
import java.nio.file.Files
import java.nio.file.Paths
import java.sql.Connection
import java.sql.DriverManager
import java.sql.PreparedStatement
import java.sql.Statement
import java.text.SimpleDateFormat
import java.time.Instant
import java.util.*
import java.util.regex.Pattern

/**
 * This worker verticle handles the processing of LLR files that are downloaded from DDS.
 */
class AlbertaLiabilityImporter : AbstractVerticle() {
    companion object {
        const val reportMonthRegex = "Rating Data.*(?<date>\\d\\d \\D\\D\\D \\d\\d\\d\\d); "
        const val wellRegex = "W (?<licence>\\d*) ; (?<status>[^;]*); (?<location>[^;]*); \\$(?<assetvalue>(([1-9]\\d{0,2}(,\\d{3})*)|(([1-9]\\d*)?\\d))(\\.\\d\\d)); \\$(?<liabilityvalue>(([1-9]\\d{0,2}(,\\d{3})*)|(([1-9]\\d*)?\\d))(\\.\\d\\d)); (?<psv>[^;]*); (?<activity>[a-zA-Z])(.*?)(?m:^(?=[\r\n]|\\z))"
        const val facilityRegex = "F(?<licence>\\d*) *; (?<status>[^;]*); (?<location>[^;]*); (?<program>[^;]*); (?<calctype>[^;]*); \\$(?<assetvalue>(([1-9]\\d{0,2}(,\\d{3})*)|(([1-9]\\d*)?\\d))(\\.\\d\\d)); \\$(?<liabilityvalue>(([1-9]\\d{0,2}(,\\d{3})*)|(([1-9]\\d*)?\\d))(\\.\\d\\d)); (?<psv>[^;]*); (?<activity>[a-zA-Z])(.*?)(?m:^(?=[\r\n]|\\z))"
        const val detailsRegex = ";;(?<detail>[^\\n]*)"
    }

    override fun start() {
        vertx.eventBus().consumer<String>("og-liability-tracker.ab_importer") { message ->
            val job = JsonObject(message.body())
            val files = job.getJsonArray("uploadedFiles")
            val company = job.getInteger("company")
            val append = job.getBoolean("append")

            vertx.executeBlocking<String>({ future ->
                val liabilities = LinkedList<AbLiability>()

                try {
                    files.forEach {
                        val file = JsonObject(it.toString())
                        val path = "${System.getProperty("user.dir")}${File.separator}${file.getString("fileName")}"

                        liabilities.addAll(parseLiabilities(path))
                    }

                    val rows = persistLiabilities(company, append, liabilities);

                    future.complete("Saved ${rows} rating records")
                } catch (t : Throwable) {
                    future.fail(t)
                }
            }, {
                if (it.succeeded())
                    message.reply(JsonObject().put("status", "success").put("message", it.result()).encode())
                else
                    message.fail(1, it.cause().toString())
            })
        }
    }

    /**
     * Using the regular expressions defined in the companion object of this class, this function reads through the text
     * file and builds a list of liabilities.
     *
     * This function is blocking, but it really shouldn't take much time to parse a file and only one file gets uploaded
     * ever month, so it likely won't be an issue.
     *
     * Exceptions are thrown
     */
    private fun parseLiabilities(path : String) : List<AbLiability> {
        val list = LinkedList<AbLiability>()
        val content = readFile(path, Charset.defaultCharset())

        val dateMatcher = Pattern.compile(reportMonthRegex, Pattern.DOTALL).matcher(content)
        val wellMatcher = Pattern.compile(wellRegex, Pattern.DOTALL).matcher(content)
        val facilityMatcher = Pattern.compile(facilityRegex, Pattern.DOTALL).matcher(content)

        val dateFormat = SimpleDateFormat("dd MMM yyyy", Locale.CANADA)
        val reportMonth : Instant

        // Every DDS file has a run date in it. This is needed for identification
        if (dateMatcher.find())
            reportMonth = Instant.ofEpochMilli(dateFormat.parse(dateMatcher.group("date")).time)
        else
            throw Throwable("File format not recognized")


        val detailsPattern = Pattern.compile(detailsRegex, Pattern.DOTALL)

        // Process the well liabilities
        while (wellMatcher.find()) {
            val well = AbLiability(
                    month = reportMonth,
                    type = "Well",
                    licence = wellMatcher.group("licence"),
                    location = wellMatcher.group("location"),
                    status = wellMatcher.group("status"),
                    assetValue = wellMatcher.group("assetvalue").replace(",", "").toDouble(),
                    liabilityValue = wellMatcher.group("liabilityvalue").replace(",", "").toDouble(),
                    psv = wellMatcher.group("psv")
            )

            val detailsMatcher = detailsPattern.matcher(wellMatcher.group(0))

            while (detailsMatcher.find()) {
                val tokens = detailsMatcher.group("detail").split(";")

                if (tokens.count() == 4 && tokens[2].trim() == "Y" && tokens[0] == "Abandonment") {
                    when (tokens[1].trim()) {
                        "Additional Event" -> well.abandonmentAdditionalEvent = tokens[3].replace("$", "").replace(",", "").toDouble()
                        "WB Abandonment" -> well.abandonmentBasic = tokens[3].replace("$", "").replace(",", "").toDouble()
                        "GWP" -> well.abandonmentGwp = tokens[3].replace("$", "").replace(",", "").toDouble()
                        "Vent Flow" -> well.abandonmentVentFlow = tokens[3].replace("$", "").replace(",", "").toDouble()
                        "Site Specific" -> well.abandonmentSiteSpecific = tokens[3].replace("$", "").replace(",", "").toDouble()
                    }
                } else if (tokens.count() == 4 && tokens[2].trim() == "Y" && tokens[0] == "Reclamation") {
                    when (tokens[1].trim()) {
                        "Site Reclamation" -> well.reclamationBasic = tokens[3].replace("$", "").replace(",", "").toDouble()
                        "Site Specific" -> well.reclamationSiteSpecific = tokens[3].replace("$", "").replace(",", "").toDouble()
                    }
                }
            }

            list.add(well)
        }

        // Run through the facility list
        while (facilityMatcher.find()) {
            val facility = AbLiability(
                    month = reportMonth,
                    type = "Facility",
                    licence = facilityMatcher.group("licence"),
                    location = facilityMatcher.group("location"),
                    status = facilityMatcher.group("status"),
                    assetValue = facilityMatcher.group("assetvalue").replace(",", "").toDouble(),
                    liabilityValue = facilityMatcher.group("liabilityvalue").replace(",", "").toDouble(),
                    calculationType = facilityMatcher.group("calctype"),
                    psv = facilityMatcher.group("psv")
            )

            val detailsMatcher = detailsPattern.matcher(facilityMatcher.group(0))

            while (detailsMatcher.find()) {
                val tokens = detailsMatcher.group("detail").split(";")

                if (tokens.count() == 4 && tokens[2].trim() == "Y" && tokens[0] == "Abandonment") {
                    when (tokens[1].trim()) {
                        "Fac Abandonment" -> facility.abandonmentBasic = tokens[3].replace("$", "").replace(",", "").toDouble()
                        "Site Specific" -> facility.abandonmentSiteSpecific = tokens[3].replace("$", "").replace(",", "").toDouble()
                    }
                } else if (tokens.count() == 4 && tokens[2].trim() == "Y" && tokens[0] == "Reclamation") {
                    when (tokens[1].trim()) {
                        "Site Reclamation" -> facility.reclamationBasic = tokens[3].replace("$", "").replace(",", "").toDouble()
                        "Site Specific" -> facility.reclamationSiteSpecific = tokens[3].replace("$", "").replace(",", "").toDouble()
                    }
                }
            }

            list.add(facility)
        }

        return list
    }


    /**
     * Takes a list of liability ratings and attempts to get them saved in the database
     */
    private fun persistLiabilities(companyId : Int, append : Boolean, liabilities : List<AbLiability>) : Int {
        Class.forName(config().getString("db.jdbc_driver"))

        var recordsPersisted : Int
        var connection : Connection? = null

        try {
            connection =  DriverManager.getConnection("${config().getString("db.url_proto")}${config().getString("db.file_path")}${config().getString("db.url_options")}", config().getString("db.username"), config().getString("db.password"))

            val provinceId = getProvince(connection)

            if (append == false)
                clearExistingRatings(connection, provinceId, companyId)

            recordsPersisted = insertLiabilities(connection, provinceId, companyId, liabilities)
        } finally {
            connection?.close()
        }

        return recordsPersisted
    }

    /**
     *  Gets the ID of the Alberta record in the database
     */
    private fun getProvince(connection : Connection) : Int {
        val findProvinceSql = "SELECT id, name, short_name FROM provinces WHERE name = 'Alberta'"
        val provinceId : Int

        var statement : Statement? = null

        try {
            statement = connection.createStatement()

            val rs = statement.executeQuery(findProvinceSql)

            if (rs.next())
                provinceId = rs.getInt(1)
            else
                throw Exception("Unable to find Alberta in the database")
        } finally {
            statement?.close()
        }

        return provinceId
    }

    /**
     * Returns a HashMap containing all of the entities under the given company and province.
     */
    private fun getExistingEntities(connection : Connection, provinceId : Int, companyId : Int) : HashMap<String, Int> {
        val allEntitiesSql = "SELECT e.id, e.type, e.licence FROM entities e WHERE e.province_id = ? and e.company_id = ?"
        val dictionary = HashMap<String, Int>()

        var statement : PreparedStatement? = null

        try {

            statement = connection.prepareStatement(allEntitiesSql)

            statement.setInt(1, provinceId)
            statement.setInt(2, companyId)

            val rs = statement.executeQuery()

            while (rs.next()) {
                dictionary.put("${rs.getString(2)}${rs.getString(3)}", rs.getInt(1))
            }
        } finally {
            statement?.close()
        }

        return dictionary
    }

    /**
     * Deletes all entity ratings from company, but leaves the entities alone
     */
    private fun clearExistingRatings(connection : Connection, provinceId : Int, companyId : Int) {
        val clearRatingsSql = "DELETE FROM entity_ratings WHERE entity_id in (SELECT id FROM entities WHERE province_id = ? AND company_id = ?)"

        var statement : PreparedStatement? = null

        try {
            statement = connection.prepareStatement(clearRatingsSql)

            statement.setInt(1, provinceId)
            statement.setInt(2, companyId)

            statement.executeUpdate()
        } finally {
            statement?.close()
        }
    }

    /**
     * Takes a list of AbLiability objects and saves them to the database. As the database is setup as entity->monthly_ratings
     * with the licence being in the entity and the monthly ratings being children, it's important that we don't duplicated
     * licences in the entity table. The easiest solution is to read all the entities into a HashMap so that we can look
     * up entity ids before inserting, but this likely isn't safe as it's possible for two different connections to insert
     * the same licence concurrently... Needs a better solution.
     */
    private fun insertLiabilities(connection : Connection, provinceId : Int, companyId : Int, liabilities : List<AbLiability>) : Int {
        val insertEntitySql = "INSERT INTO entities (province_id, company_id, type, licence, location_identifier) VALUES (?, ?, ?, ?, ?)"
        val insertLiabilitySql = "INSERT INTO entity_ratings (entity_id, report_month, entity_status, calculation_type, pvs_value_type, asset_value, liability_value, abandonment_basic, abandonment_additional_event, abandonment_gwp, abandonment_gas_migration, abandonment_vent_flow, abandonment_site_specific, reclamation_basic, reclamation_site_specific) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"

        val autoCommitBackup = connection.autoCommit
        val savedRatingsCount : Int

        var entityLookup = getExistingEntities(connection, provinceId, companyId)

        var entityStatement : PreparedStatement? = null
        var liabilityStatement : PreparedStatement? = null

        try {
            connection.autoCommit = false

            entityStatement = connection.prepareStatement(insertEntitySql)
            liabilityStatement = connection.prepareStatement(insertLiabilitySql)

            for (item in liabilities) {
                if (!entityLookup.contains("${item.type}${item.licence}")) {
                    entityStatement.setInt(1, provinceId)
                    entityStatement.setInt(2, companyId)
                    entityStatement.setString(3, item.type)
                    entityStatement.setString(4, item.licence)
                    entityStatement.setString(5, item.location)

                    entityStatement.addBatch()
                }
            }

            entityStatement.executeBatch()

            connection.commit()

            // Now that we have added all of the missing liabilities we can refresh the lookup
            entityLookup = getExistingEntities(connection, provinceId, companyId)

            for (item in liabilities) {
                val pk = entityLookup.get("${item.type}${item.licence}") ?: throw Exception("Unable to get an entity match on one or more ratings")

                liabilityStatement.setInt(1, pk)
                liabilityStatement.setLong(2, item.month.toEpochMilli())
                liabilityStatement.setString(3, item.status)
                if (item.calculationType != null) liabilityStatement.setString(4, item.calculationType)
                liabilityStatement.setString(5, item.psv)
                liabilityStatement.setDouble(6, item.assetValue)
                liabilityStatement.setDouble(7, item.liabilityValue)
                liabilityStatement.setDouble(8, item.abandonmentBasic)
                liabilityStatement.setDouble(9, item.abandonmentAdditionalEvent)
                liabilityStatement.setDouble(10, item.abandonmentGwp)
                liabilityStatement.setDouble(11, item.abandonmentGasMigration)
                liabilityStatement.setDouble(12, item.abandonmentVentFlow)
                liabilityStatement.setDouble(13, item.abandonmentSiteSpecific)
                liabilityStatement.setDouble(14, item.reclamationBasic)
                liabilityStatement.setDouble(15, item.reclamationSiteSpecific)

                liabilityStatement.addBatch()
            }

            savedRatingsCount = liabilityStatement.executeBatch().size

            connection.commit()
        } finally {
            entityStatement?.close()
            liabilityStatement?.close()

            connection.autoCommit = autoCommitBackup
        }

        return savedRatingsCount
    }

    /**
     * Reads a file into a string. The file should be relatively small unless you want to use a lot of memory space...
     */
    private fun readFile(path : String, encoding: Charset) : String {
        val bytes = Files.readAllBytes(Paths.get(path));

        return String(bytes, encoding)
    }

}