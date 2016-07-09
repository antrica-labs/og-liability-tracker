CREATE TABLE historical_netbacks (
    id IDENTITY PRIMARY KEY,
    province_id TIMESTAMP,
    effective_date DATE,
    netback DOUBLE,
    shrinkage_factor DOUBLE,
    oil_equivalent_conversion DOUBLE
)