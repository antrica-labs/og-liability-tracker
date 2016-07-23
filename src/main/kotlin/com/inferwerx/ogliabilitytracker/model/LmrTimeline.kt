package com.inferwerx.ogliabilitytracker.model

import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import java.time.LocalDate
import java.time.ZoneId
import java.util.*

class LmrTimeline {
    val timeline = HashMap<LocalDate, LmrRating>()

    constructor() {}

    constructor(array: JsonArray) {
        array.forEach {
            val json = it as JsonObject

            val date = json.getInstant("report_date").atZone(ZoneId.systemDefault()).toLocalDate()
            val rating = LmrRating(json.getJsonObject("rating"))

            timeline.put(date, rating)
        }
    }

    fun replacePoint(date: LocalDate, rating: LmrRating) {
        timeline.put(date, rating)
    }

    fun mergePoint(date: LocalDate, rating: LmrRating) {
        if (timeline.containsKey(date))
            timeline.put(date, timeline[date]!!.plusRating(rating))
        else
            timeline.put(date, rating)
    }

    fun mergeTimeline(lmr: LmrTimeline) {
        lmr.timeline.forEach { date, rating ->
            this.mergePoint(date, rating)
        }
    }

    fun remotePoint(date: LocalDate) {
        timeline.remove(date)
    }

    fun toJson(): JsonArray {
        val result = JsonArray()

        timeline.keys.sorted().forEach {
            result.add(timeline[it]!!.toJson().put("report_date", it.atStartOfDay(ZoneId.systemDefault()).toInstant()))
        }

        return result
    }
}