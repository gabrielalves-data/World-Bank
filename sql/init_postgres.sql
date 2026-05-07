-- Create analytics database (if not already exists)
CREATE DATABASE IF NOT EXISTS analytics;

-- Connect to analytics database
\c analytics;


-- Staging Table
CREATE TABLE IF NOT EXISTS stg_gdp (
    country_name VARCHAR(100),
    country_iso3 VARCHAR(3),
    indicator_id VARCHAR(20),
    year INT,
    gdp_usd DOUBLE PRECISION
);


-- Dimension: country
CREATE TABLE IF NOT EXISTS dim_country (
    country_key VARCHAR(3) PRIMARY KEY,
    country_name VARCHAR(100) NOT NULL,
    region VARCHAR(100),
    income_level VARCHAR(50)
);

-- Dimension: time
CREATE TABLE IF NOT EXISTS dim_time (
    time_key INT PRIMARY KEY,
    year INT NOT NULL,
    decade INT NOT NULL,
    era VARCHAR(20)
);

-- Dimension: indicator
CREATE TABLE IF NOT EXISTS dim_indicator (
    indicator_key VARCHAR(20) PRIMARY KEY,
    indicator_name VARCHAR(100)
);

-- Fact Table
CREATE TABLE IF NOT EXISTS fact_gdp (
    measurement_id BIGSERIAL PRIMARY KEY,
    country_key VARCHAR(3) NOT NULL REFERENCES dim_country(country_key) ON DELETE CASCADE,
    time_key INT NOT NULL REFERENCES dim_time(time_key) ON DELETE CASCADE,
    indicator_key VARCHAR(20) NOT NULL REFERENCES dim_indicator(indicator_key) ON DELETE CASCADE,
    gdp_usd DOUBLE PRECISION,
    loaded_at TIMESTAMP DEFAULT NOW(),
    CONSTRAINT unique_gdp_measurement UNIQUE (country_key, time_key, indicator_key)
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_fact_country ON fact_gdp(country_key);
CREATE INDEX IF NOT EXISTS idx_fact_time ON fact_gdp(time_key);
CREATE INDEX IF NOT EXISTS idx_fact_indicator ON fact_gdp(indicator_key);