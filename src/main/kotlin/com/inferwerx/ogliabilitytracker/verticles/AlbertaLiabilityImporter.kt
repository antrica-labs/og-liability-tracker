package com.inferwerx.ogliabilitytracker.verticles

import com.inferwerx.ogliabilitytracker.alberta.AbLiability
import io.vertx.core.AbstractVerticle
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.ext.jdbc.JDBCClient
import java.io.File
import java.nio.charset.Charset
import java.nio.file.Files
import java.nio.file.Paths
import java.text.SimpleDateFormat
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
            val file = JsonObject(message.body())

            val path = "${System.getProperty("user.dir")}${File.separator}${file.getString("fileName")}"

            try {
                val liabilities = parseLiabilities(path)

                persistLiabilities(file.getInteger("companyId"), file.getBoolean("append"), liabilities)

                message.reply(JsonObject().put("file", file.getString("originalFileName")).put("status", "parsed").put("message", "Processed ${liabilities.count()} licences").encode())
            } catch (e : Exception) {
                message.reply(JsonObject().put("file", file.getString("originalFileName")).put("status", "failed").put("message", e.cause.toString()).encode())
            }
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
        val reportMonth : Date

        // Every DDS file has a run date in it. This is needed for identification
        if (dateMatcher.find())
            reportMonth = java.sql.Date(dateFormat.parse(dateMatcher.group("date")).time)
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

    private fun persistLiabilities(companyId : Int, append : Boolean, liabilities : List<AbLiability>) {
        val dbConfig = JsonObject()
                .put("driver_class", config().getString("db.jdbc_driver"))
                .put("url", "${config().getString("db.url_proto")}${config().getString("db.file_path")}${config().getString("db.url_options")}")
                .put("user", config().getString("db.username"))
                .put("password", config().getString("db.password"))
        val dbClient = JDBCClient.createShared(vertx, dbConfig)

        dbClient.getConnection { connection ->
            if (connection.failed())
                throw Throwable(connection.cause())

            val db = connection.result()



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