CREATE TABLE provinces (
  id INTEGER PRIMARY KEY,
  name VARCHAR(255),
  short_name VARCHAR(2)
);

CREATE TABLE company (
    id INTEGER PRIMARY KEY,
    name VARCHAR(255),
    alt_name VARCHAR(255)
);

CREATE TABLE entity (
  id INTEGER PRIMARY KEY,
  province_id INTEGER,
  licence VARCHAR(255),
  location_identifier VARCHAR(255)
);

CREATE TABLE entity_info (
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
);

CREATE TABLE disposed_entities (
  id INTEGER PRIMARY KEY,
  entity_id INTEGER,
  effective_date DATE,
  created_date DATE
);

INSERT INTO provinces (name, short_name) VALUES ('Alberta', 'AB');
INSERT INTO provinces (name, short_name) VALUES ('British Columbia', 'BC');