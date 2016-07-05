package com.inferwerx.ogliabilitytracker.verticles.workers

import io.vertx.core.AbstractVerticle
import io.vertx.core.json.JsonObject

class LiabilityExporter : AbstractVerticle() {
    private val provinceQuery = "SELECT name, short_name FROM provinces WHERE province_id = ?"
    private val companyQuery = "SELECT name, alt_name FROM companies WHERE company_id = ?"
    private val liabilityDetailsQuery = """
        SELECT
          e.type,
          e.licence,
          e.location_identifier,
          h.hierarchy_value,
          date(r.report_month, 'unixepoch') AS report_month,
          r.entity_status,
          r.calculation_type,
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


        }
    }

    data class ExportRecord (
            val hierarchyElement : String,
            val type : String,
            val licence : String,
            val status : String,
            val location : String,
            val assetValue : Double,
            val liabilityValue : Double,
            val psv : String,
            val active : String,
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