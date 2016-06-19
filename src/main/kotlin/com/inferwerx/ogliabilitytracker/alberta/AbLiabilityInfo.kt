package com.inferwerx.ogliabilitytracker.alberta

import java.sql.Date


data class AbLiabilityInfo (
        var month : Date,
        var status : String,
        var calculationType : String,
        var assetValue : Double,
        var liabilityValue : Double,
        var abandonmentBasic : Double,
        var abandonmentAdditionalEvent : Double,
        var abandonmentGwp : Double,
        var abandonmentGasMigration : Double,
        var abandonmentVentFlow : Double,
        var abandonmentSiteSpecific : Double,
        var reclamationBasic : Double,
        var reclamationSiteSpecific : Double
)
