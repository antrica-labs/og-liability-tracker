package com.inferwerx.ogliabilitytracker.model

import io.vertx.core.json.JsonObject

class LmrRating {
    var assetValue: Double
    var liabilityAmount: Double

    constructor(assetValue: Double, liabilityAmount: Double) {
        this.assetValue = assetValue
        this.liabilityAmount = liabilityAmount
    }

    constructor(json: JsonObject) {
        this.assetValue = json.getDouble("asset_value")
        this.liabilityAmount = json.getDouble("liability_amount")
    }

    val netValue: Double
        get() = assetValue - liabilityAmount

    val rating: Double
        get() = if (liabilityAmount == 0.0) -1.0 else assetValue / liabilityAmount

    fun plusRating(rating: LmrRating): LmrRating = LmrRating(assetValue + rating.assetValue, liabilityAmount + rating.liabilityAmount)

    fun toJson(): JsonObject = JsonObject()
        .put("asset_value", assetValue)
        .put("liability_amount", liabilityAmount)
        .put("rating", rating)
        .put("net_value", netValue)
}
