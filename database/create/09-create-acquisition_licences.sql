CREATE TABLE acquisition_licences (
  id IDENTITY PRIMARY KEY,
  aquisition_id INTEGER,
  type VARCHAR(255),
  licence INTEGER,
  liability_amount DOUBLE
)