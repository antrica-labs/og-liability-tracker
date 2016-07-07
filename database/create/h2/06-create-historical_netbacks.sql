CREATE TABLE historical_netbacks (
    id IDENTITY PRIMARY KEY,
    province_id INTEGER,
    effective_date DATE,
    netback DOUBLE,
    shrinkage_factor DOUBLE,
    oil_equivalent_conversion DOUBLE
)