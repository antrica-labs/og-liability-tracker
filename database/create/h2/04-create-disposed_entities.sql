CREATE TABLE disposed_entities (
  id IDENTITY PRIMARY KEY,
  type VARCHAR(255),
  licence INTEGER,
  effective_date DATE,
  created_date DATE
)