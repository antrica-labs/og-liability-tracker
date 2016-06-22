CREATE TABLE entity_ratings (
  id INTEGER PRIMARY KEY,
  entity_id INTEGER,
  report_month DATE,
  entity_status VARCHAR(255),
  calculation_type VARCHAR(255),
  pvs_value_type VARCHAR(255),
  asset_value DOUBLE DEFAULT 0,
  liability_value DOUBLE DEFAULT 0,
  abandonment_basic DOUBLE DEFAULT 0,
  abandonment_additional_event DOUBLE DEFAULT 0,
  abandonment_gwp DOUBLE DEFAULT 0,
  abandonment_gas_migration DOUBLE DEFAULT 0,
  abandonment_vent_flow DOUBLE DEFAULT 0,
  abandonment_site_specific DOUBLE DEFAULT 0,
  reclamation_basic DOUBLE DEFAULT 0,
  reclamation_site_specific DOUBLE DEFAULT 0
)