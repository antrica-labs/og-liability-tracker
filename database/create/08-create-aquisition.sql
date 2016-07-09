CREATE TABLE aquisition (
  id IDENTITY PRIMARY KEY,
  active BOOLEAN,
  description VARCHAR(255),
  effective_date TIMESTAMP,
  purchase_price DOUBLE
)