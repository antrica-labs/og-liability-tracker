package com.inferwerx.ogliabilitytracker.alberta

data class AbLiability (
        var month : java.sql.Date,
        var type : String,
        var licence : String,
        var location : String,
        var status : String,
        var calculationType : String? = null,
        var assetValue : Double,
        var liabilityValue : Double,
        var psv : String,
        var abandonmentBasic : Double = 0.0,
        var abandonmentAdditionalEvent : Double = 0.0,
        var abandonmentGwp : Double = 0.0,
        var abandonmentGasMigration : Double = 0.0,
        var abandonmentVentFlow : Double = 0.0,
        var abandonmentSiteSpecific : Double = 0.0,
        var reclamationBasic : Double = 0.0,
        var reclamationSiteSpecific : Double = 0.0
)
