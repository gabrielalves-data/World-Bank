-- World Bank GDP Pipeline - Staging → Dimensions → Facts
-- Run this after Spark has loaded stg_gdp from S3 Parquet.
-- Every statement is idempotent (ON CONFLICT DO NOTHING / DO UPDATE).


-- -----------------------------------------------------------------------------
-- STEP 1: Populate dim_country from staging
-- Only inserts rows that don't already exist.
-- country_name is updated if it changes (e.g. country renamed).
-- region / income_level are NOT touched here — enriched separately via API.
-- -----------------------------------------------------------------------------
INSERT INTO dim_country (country_key, country_name)
SELECT DISTINCT
    country_iso3 AS country_key,
    MAX(country_name) AS country_name
FROM stg_gdp
WHERE country_iso3 IS NOT NULL
GROUP BY country_iso3
ON CONFLICT(country_key) DO UPDATE
    SET country_name = EXCLUDED.country_name;


-- -----------------------------------------------------------------------------
-- STEP 2: Populate dim_indicator from staging
-- -----------------------------------------------------------------------------
INSERT INTO dim_indicator(indicator_key, indicator_name)
SELECT DISTINCT
    indicator_id AS indicator_key,
    MAX(indicator_id) AS indicator_name
FROM stg_gdp
WHERE indicator_id IS NOT NULL
GROUP BY indicator_id
ON CONFLICT(indicator_key) DO NOTHING;


-- -----------------------------------------------------------------------------
-- STEP 3: Populate dim_time
-- Covers every year present in staging.
-- Existing rows are left untouched.
-- -----------------------------------------------------------------------------
INSERT INTO dim_time(time_key, year, decade, era)
SELECT DISTINCT ON (year)
    year AS time_key,
    year,
    (year / 10) * 10 AS decade,
    CASE
        WHEN year < 1990 THEN 'Pre-1990'
        WHEN year BETWEEN 1990 AND 1999 THEN '1990s'
        WHEN year BETWEEN 2000 AND 2009 THEN '2000s'
        WHEN year BETWEEN 2010 AND 2019 THEN '2010s'
        ELSE '2020s'
    END AS era
FROM stg_gdp
WHERE year IS NOT NULL
ON CONFLICT(time_key) DO NOTHING;


-- -----------------------------------------------------------------------------
-- STEP 4: Load fact_gdp_measurements from staging
-- ON CONFLICT: if the row already exists (same country/year/indicator),
-- we update gdp_usd in case the source value was revised.
-- -----------------------------------------------------------------------------
INSERT INTO fact_gdp (country_key, time_key, indicator_key, gdp_usd)
WITH deduped AS (
    SELECT
        country_iso3  AS country_key,
        year          AS time_key,
        indicator_id  AS indicator_key,
        MAX(gdp_usd)  AS gdp_usd
    FROM stg_gdp
    WHERE country_iso3 IS NOT NULL
      AND year         IS NOT NULL
      AND indicator_id IS NOT NULL
    GROUP BY country_iso3, year, indicator_id
)
SELECT d.*
FROM deduped d
WHERE EXISTS (SELECT 1 FROM dim_country   WHERE country_key  = d.country_key)
  AND EXISTS (SELECT 1 FROM dim_time      WHERE time_key     = d.time_key)
  AND EXISTS (SELECT 1 FROM dim_indicator WHERE indicator_key = d.indicator_key)
ON CONFLICT (country_key, time_key, indicator_key) DO UPDATE
    SET gdp_usd   = EXCLUDED.gdp_usd,
        loaded_at = NOW();


-- -----------------------------------------------------------------------------
-- STEP 5: Clear staging (keeps the table ready for the next run)
-- -----------------------------------------------------------------------------
TRUNCATE TABLE stg_gdp;