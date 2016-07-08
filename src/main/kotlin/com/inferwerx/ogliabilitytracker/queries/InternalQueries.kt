package com.inferwerx.ogliabilitytracker.queries

class InternalQueries {
    companion object Factory {
        val GET_ALL_PROVINCES = "SELECT id, name, short_name FROM provinces ORDER BY name"
        val GET_PROVINCE_BY_NAME = "SELECT id, name, short_name FROM provinces WHERE name = ?"
        val GET_PROVINCE_BY_ID = "SELECT name, short_name FROM provinces WHERE id = ?"

        val GET_ALL_COMPANIES = "SELECT id, name, alt_name FROM companies ORDER BY name"
        val GET_COMPANY_BY_ID = "SELECT name, alt_name FROM companies WHERE id = ?"

        val GET_ENTITIES_BY_COMPANY_AND_PROVINCE = "SELECT e.id, e.type, e.licence FROM entities e WHERE e.province_id = ? and e.company_id = ?"
        val DELETE_RATINGS_BY_COMPANY_AND_PROVINCE = "DELETE FROM entity_ratings WHERE entity_id in (SELECT id FROM entities WHERE province_id = ? AND company_id = ?)"

        val INSERT_ENTITY = "INSERT INTO entities (province_id, company_id, type, licence, location_identifier) VALUES (?, ?, ?, ?, ?)"
        val INSERT_RATING = "INSERT INTO entity_ratings (entity_id, report_month, entity_status, calculation_type, pvs_value_type, asset_value, liability_value, abandonment_basic, abandonment_additional_event, abandonment_gwp, abandonment_gas_migration, abandonment_vent_flow, abandonment_site_specific, reclamation_basic, reclamation_site_specific) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"

        val INSERT_HIERARCHY_LOOKUP = "INSERT INTO hierarchy_lookup (company_id, type, licence, hierarchy_value) VALUES (?, ?, ?, ?)"
        val DELETE_HIERARCHY_LOOKUPS = "DELETE FROM hierarchy_lookup WHERE company_id = ?"

        val GET_PROFORMA_HISTORY = """
            SELECT
              r.report_month,
              sum(r.asset_value)                AS asset_value,
              sum(r.liability_value)            AS liability_value
            FROM entity_ratings r INNER JOIN entities e ON e.id = r.entity_id
            WHERE e.id IN (
              SELECT entity_id
              FROM entity_ratings
              WHERE report_month IN (SELECT max(report_month) latest_month
                                     FROM entity_ratings))
                  AND e.province_id = ?
                  AND e.company_id = ?
            GROUP BY r.report_month
            ORDER BY r.report_month
        """

        val GET_HISTORY = """
            SELECT
              r.report_month,
              sum(r.asset_value)                AS asset_value,
              sum(r.liability_value)            AS liability_value
            FROM entity_ratings r INNER JOIN entities e ON e.id = r.entity_id
            WHERE e.province_id = ?
                  AND e.company_id = ?
            GROUP BY r.report_month
            ORDER BY r.report_month
        """

        val GET_REPORT_DATES = """
            SELECT DISTINCT report_month
            FROM entity_ratings r INNER JOIN entities e ON r.entity_id = e.id
            WHERE e.province_id = ? AND e.company_id = ?
            ORDER BY report_month DESC
        """

        val GET_NETBACKS = "SELECT effective_date, netback, shrinkage_factor, oil_equivalent_conversion FROM historical_netbacks WHERE province_id = ? ORDER BY effective_date ASC"

        val GET_REPORT_DETAILS = """
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
    }
}