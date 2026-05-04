CREATE TABLE exchange_rates (
    currency_name  TEXT,
    exchange_rate  FLOAT,
    currency_code  TEXT,
    exchange_date  DATE,
    updated_at     TIMESTAMP
);

COPY exchange_rates(currency_name, exchange_rate, currency_code, exchange_date, updated_at)
FROM 'D:\Work\tech_projects\laba_tech\exchange_rates.csv'
DELIMITER ','
CSV HEADER;