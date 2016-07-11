CREATE TABLE dispositions (
  id IDENTITY PRIMARY KEY,
  province_id INTEGER,
  active BOOLEAN,
  description VARCHAR(255),
  effective_date TIMESTAMP,
  sale_price DOUBLE
)