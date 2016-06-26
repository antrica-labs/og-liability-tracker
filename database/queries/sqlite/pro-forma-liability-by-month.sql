SELECT
  date(report_month, 'unixepoch') AS report_month,
  sum(asset_value)                AS asset_value,
  sum(liability_value)            AS liability_value
FROM entity_ratings
WHERE entity_id IN (
  SELECT entity_id
  FROM entity_ratings
  WHERE report_month IN (SELECT max(report_month) latest_month
                         FROM entity_ratings))
  AND report_month >= ?
  AND report_month <= ?
GROUP BY report_month
ORDER BY report_month