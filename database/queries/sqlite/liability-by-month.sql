SELECT
  date(report_month, 'unixepoch') AS report_month,
  sum(asset_value)                AS asset_value,
  sum(liability_value)            AS liability_value
FROM entity_ratings
WHERE r.report_month >= ? and r.report_month <= ?
GROUP BY report_month
ORDER BY report_month




