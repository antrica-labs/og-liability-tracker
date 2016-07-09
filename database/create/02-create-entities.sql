CREATE TABLE entities (
  id IDENTITY PRIMARY KEY,
  province_id INTEGER,
  type VARCHAR(255),
  licence INTEGER,
  location_identifier VARCHAR(255)
)