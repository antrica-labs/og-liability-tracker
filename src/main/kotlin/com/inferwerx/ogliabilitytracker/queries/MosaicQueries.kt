package com.inferwerx.ogliabilitytracker.queries

class MosaicQueries {
    companion object Factory {
        val GET_GROWTH_PRODUCTION = """
            SELECT
              ee.entity_name AS "entity_name",
              to_date(ed.year || '-' || case when ed.month = 0 then '01' else lpad(ed.month, 2, '0') end || '-01', 'yyyy-mm-dd') AS "production_month",
              ed.gas_pool_volume / 35.4937 AS "gas_volume",
              ed.oil_pool_volume / 6.28981 AS "oil_volume"
            FROM
              economic_result er
            INNER JOIN reserve_category rc
            ON
              rc.reserve_category_id = er.reserve_category_id
            INNER JOIN economic_case ec
            ON
              er.case_id = ec.case_id
            INNER JOIN company c
            ON
              er.company_id = c.company_id
            INNER JOIN economic_detail ed
            ON
              er.result_id = ed.result_id
            INNER JOIN economic_entity ee
            ON
              ee.entity_id = er.entity_id
            WHERE
              c.company_name        = 'Spyglass'
            AND ec.case_name        = 'Actuals'
            AND rc.category_name    = 'G2'
            AND ee.operated         = 1
            AND ee.province         = 'Alberta'
            AND ee.entity_state_id IN (1, 0)
            AND er.risk            IN ('B', 'N')
            AND to_date(ed.YEAR || '-' || CASE WHEN ed.MONTH = 0 THEN '01' ELSE lpad(ed.MONTH, 2, '0') END || '-01', 'yyyy-mm-dd') BETWEEN ? AND add_months(?, 12)
            ORDER BY
              ee.entity_name,
              ed.year,
              ed.month
        """

        val GET_ENTITY_LIST = """
            SELECT
              entity_name AS "entity_name",
              MIN(production_month) AS "start_date"
            FROM
              (
                SELECT
                  ee.entity_name,
                  to_date(ed.year || '-' || case when ed.month = 0 then '01' else lpad(ed.month, 2, '0') end || '-01', 'yyyy-mm-dd') as production_month,
                  ed.gas_pool_volume / 35.4937 AS gas_volume,
                  ed.oil_pool_volume / 6.28981 AS oil_volume
                FROM
                  economic_result er
                INNER JOIN reserve_category rc
                ON
                  rc.reserve_category_id = er.reserve_category_id
                INNER JOIN economic_case ec
                ON
                  er.case_id = ec.case_id
                INNER JOIN company c
                ON
                  er.company_id = c.company_id
                INNER JOIN economic_detail ed
                ON
                  er.result_id = ed.result_id
                INNER JOIN economic_entity ee
                ON
                  ee.entity_id = er.entity_id
                WHERE
                  c.company_name        = 'Spyglass'
                AND ec.case_name        = 'Actuals'
                AND rc.category_name    = 'G2'
                AND ee.operated         = 1
                AND ee.province         = 'Alberta'
                AND ee.entity_state_id IN (1, 0)
                AND er.risk            IN ('B', 'N')
                AND to_date(ed.YEAR || '-' || CASE WHEN ed.MONTH = 0 THEN '01' ELSE lpad(ed.MONTH, 2, '0') END || '-01', 'yyyy-mm-dd') BETWEEN ? AND add_months(?, 12)
                ORDER BY
                  ee.entity_name,
                  ed.year,
                  ed.month
              )
            WHERE
              gas_volume  > 0
            OR oil_volume > 0
            GROUP BY
              entity_name
            ORDER BY
              "start_date" DESC
        """
    }
}