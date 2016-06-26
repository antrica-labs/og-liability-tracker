SELECT
  e.type,
  e.licence,
  e.location_identifier,
  date(r.report_month, 'unixepoch') AS report_month,
  r.entity_status,
  r.asset_value,
  r.liability_value,
  r.abandonment_basic,
  r.abandonment_additional_event,
  r.abandonment_gas_migration,
  r.abandonment_gwp,
  r.abandonment_vent_flow,
  r.abandonment_site_specific,
  r.reclamation_basic,
  r.reclamation_site_specific
FROM entities e INNER JOIN entity_ratings r ON r.entity_id = e.id
WHERE r.report_month >= ? and r.report_month <= ?
ORDER BY r.report_month, e.type, e.licence

