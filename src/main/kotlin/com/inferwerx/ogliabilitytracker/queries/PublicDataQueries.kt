package com.inferwerx.ogliabilitytracker.queries;

class PublicDataQueries {
    companion object Factory {
        val GET_HISTORICAL_VOLUMES_BY_LICENCE = """
            SELECT
              pd.YEAR,
              sum(CASE WHEN (pd.product_type = 'P-GAS') THEN nvl(pd.jan_volume, 0.0) END) AS jan_gas_volume,
              sum(CASE WHEN (pd.product_type = 'P-OIL') THEN nvl(pd.jan_volume, 0.0) END) AS jan_oil_volume,
              sum(CASE WHEN (pd.product_type = 'P-GAS') THEN nvl(pd.feb_volume, 0.0) END) AS feb_gas_volume,
              sum(CASE WHEN (pd.product_type = 'P-OIL') THEN nvl(pd.feb_volume, 0.0) END) AS feb_oil_volume,
              sum(CASE WHEN (pd.product_type = 'P-GAS') THEN nvl(pd.mar_volume, 0.0) END) AS mar_gas_volume,
              sum(CASE WHEN (pd.product_type = 'P-OIL') THEN nvl(pd.mar_volume, 0.0) END) AS mar_oil_volume,
              sum(CASE WHEN (pd.product_type = 'P-GAS') THEN nvl(pd.apr_volume, 0.0) END) AS apr_gas_volume,
              sum(CASE WHEN (pd.product_type = 'P-OIL') THEN nvl(pd.apr_volume, 0.0) END) AS apr_oil_volume,
              sum(CASE WHEN (pd.product_type = 'P-GAS') THEN nvl(pd.may_volume, 0.0) END) AS may_gas_volume,
              sum(CASE WHEN (pd.product_type = 'P-OIL') THEN nvl(pd.may_volume, 0.0) END) AS may_oil_volume,
              sum(CASE WHEN (pd.product_type = 'P-GAS') THEN nvl(pd.jun_volume, 0.0) END) AS jun_gas_volume,
              sum(CASE WHEN (pd.product_type = 'P-OIL') THEN nvl(pd.jun_volume, 0.0) END) AS jun_oil_volume,
              sum(CASE WHEN (pd.product_type = 'P-GAS') THEN nvl(pd.jul_volume, 0.0) END) AS jul_gas_volume,
              sum(CASE WHEN (pd.product_type = 'P-OIL') THEN nvl(pd.jul_volume, 0.0) END) AS jul_oil_volume,
              sum(CASE WHEN (pd.product_type = 'P-GAS') THEN nvl(pd.aug_volume, 0.0) END) AS aug_gas_volume,
              sum(CASE WHEN (pd.product_type = 'P-OIL') THEN nvl(pd.aug_volume, 0.0) END) AS aug_oil_volume,
              sum(CASE WHEN (pd.product_type = 'P-GAS') THEN nvl(pd.sep_volume, 0.0) END) AS sep_gas_volume,
              sum(CASE WHEN (pd.product_type = 'P-OIL') THEN nvl(pd.sep_volume, 0.0) END) AS sep_oil_volume,
              sum(CASE WHEN (pd.product_type = 'P-GAS') THEN nvl(pd.oct_volume, 0.0) END) AS oct_gas_volume,
              sum(CASE WHEN (pd.product_type = 'P-OIL') THEN nvl(pd.oct_volume, 0.0) END) AS oct_oil_volume,
              sum(CASE WHEN (pd.product_type = 'P-GAS') THEN nvl(pd.nov_volume, 0.0) END) AS nov_gas_volume,
              sum(CASE WHEN (pd.product_type = 'P-OIL') THEN nvl(pd.nov_volume, 0.0) END) AS nov_oil_volume,
              sum(CASE WHEN (pd.product_type = 'P-GAS') THEN nvl(pd.dec_volume, 0.0) END) AS dec_gas_volume,
              sum(CASE WHEN (pd.product_type = 'P-OIL') THEN nvl(pd.dec_volume, 0.0) END) AS dec_oil_volume
            FROM
              pden_vol_by_month pd INNER JOIN
              well_license wl
                ON pd.pden_id = wl.uwi
            WHERE
              pd.product_type IN ('P-OIL', 'P-GAS') AND pd.year >= ? AND wl.license_id IN (%%IN_CLAUSE%%)
            GROUP BY
              pd.YEAR
            ORDER BY
              pd.YEAR DESC
        """
    }
}
