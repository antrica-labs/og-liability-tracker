CREATE TABLE acquisitions (
  id IDENTITY PRIMARY KEY,
  province_id INTEGER,
  active BOOLEAN,
  description VARCHAR(255),
  effective_date TIMESTAMP,
  purchase_price DOUBLE
)