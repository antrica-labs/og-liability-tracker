CREATE TABLE acquisition_licences (
  id IDENTITY PRIMARY KEY,
  acquisition_id INTEGER,
  type VARCHAR(255),
  licence INTEGER,
  liability_amount DOUBLE
)