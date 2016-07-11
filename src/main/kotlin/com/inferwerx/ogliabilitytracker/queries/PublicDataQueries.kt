package com.inferwerx.ogliabilitytracker.queries;

class PublicDataQueries {
    companion object Factory {
        val GET_HISTORICAL_VOLUMES_BY_LICENCE = """
        SELECT
          pd.product_type,
          pd.year,
          sum(pd.jan_volume) AS jan_volume,
          sum(pd.feb_volume) AS feb_volume,
          sum(pd.mar_volume) AS mar_volume,
          sum(pd.apr_volume) AS apr_volume,
          sum(pd.may_volume) AS may_volume,
          sum(pd.jun_volume) AS jun_volume,
          sum(pd.jul_volume) AS jul_volume,
          sum(pd.aug_volume) AS aug_volume,
          sum(pd.sep_volume) AS sep_volume,
          sum(pd.oct_volume) AS oct_volume,
          sum(pd.nov_volume) AS nov_volume,
          sum(pd.dec_volume) AS dec_volume
        FROM PDEN_VOL_BY_MONTH pd INNER JOIN well_license wl ON pd.pden_id = wl.uwi
        WHERE pd.product_type IN ('P-OIL', 'P-GAS') AND pd.year >= ? AND wl.license_id = ?
        GROUP BY pd.PRODUCT_TYPE, pd.year
        ORDER BY pd.year desc
        """
    }
}
