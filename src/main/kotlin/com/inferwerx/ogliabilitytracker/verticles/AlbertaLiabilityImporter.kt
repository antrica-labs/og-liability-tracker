package com.inferwerx.ogliabilitytracker.verticles

import com.inferwerx.ogliabilitytracker.alberta.AbLiability
import io.vertx.core.AbstractVerticle
import io.vertx.core.json.JsonObject
import java.io.File
import java.nio.charset.Charset
import java.nio.file.Files
import java.nio.file.Paths
import java.text.DateFormat
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

            val path = "${System.getProperty("user.dir")}${File.separator}${file.getString("uploadedFileName")}"

            try {
                parseLiabilities(path)
            } catch (e : Exception) {
                // reply stating the file isn't valid
            }
        }
    }

    private fun parseLiabilities(path : String) : List<AbLiability> {
        val list = LinkedList<AbLiability>()
        val content = readFile(path, Charset.defaultCharset())

        val dateMatcher = Pattern.compile(reportMonthRegex, Pattern.DOTALL).matcher(content)
        val wellMatcher = Pattern.compile(wellRegex, Pattern.DOTALL).matcher(content)
        val facilityMatcher = Pattern.compile(facilityRegex, Pattern.DOTALL).matcher(content)

        val dateFormat = SimpleDateFormat("dd MMM yyyy", Locale.CANADA)
        val reportMonth : Date

        if (dateMatcher.find())
            reportMonth = dateFormat.parse(dateMatcher.group("date"))
        else
            throw Throwable("File format not recognized")


        val destailsPattern = Pattern.compile(detailsRegex, Pattern.DOTALL)

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

            val detailsMatcher = destailsPattern.matcher(wellMatcher.group(0))

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

            val detailsMatcher = destailsPattern.matcher(facilityMatcher.group(0))

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
     * Reads a file into a string. The file should be relatively small unless you want to use a lot of memory space...
     */
    private fun readFile(path : String, encoding: Charset) : String {
        val bytes = Files.readAllBytes(Paths.get(path));

        return String(bytes, encoding)
    }

}