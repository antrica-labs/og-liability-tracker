CREATE TABLE aro_plans (
  id IDENTITY PRIMARY KEY,
  province_id INTEGER,
  active BOOLEAN,
  description VARCHAR(255),
  effective_date TIMESTAMP,
  reduction_amount DOUBLE,
  cost DOUBLE,
  comments VARCHAR(2000)
)