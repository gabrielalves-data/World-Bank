import logging
import sys
from typing import Dict, Any
from pathlib import Path
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, avg, count, countDistinct, max as spark_max, min as spark_min, sum as spark_sum, when
from pyspark.sql.types import IntegerType, DoubleType
from datetime import datetime
import psycopg2
import os
import json

from utils.config_loader import load_config
from utils.logging_config import setup_logging
from utils.config_loader import get_s3_path


# Custom Exception

class DataQualityError(Exception):
    """
    Raised when one or more critical data quality checks fail.
 
    Attributes
    ----------
    message : str
        Human-readable description of which checks failed.
    failed_checks : list[str]
        Names of the checks that triggered the failure.
    report : dict
        Full data quality report at the time of failure.
    """

    def __init__(self, message: str, failed_checks: list, report: dict):
        super().__init__(message)
        self.failed_checks = failed_checks
        self.report = report


# Constants

GDP_MIN_SUSPICIOUS: float = 1_000_000           # USD 1 million
GDP_MAX_SUSPICIOUS: float = 30_000_000_000_000  # USD 30 trillion
NULL_RATE_THRESHOLD: float = 0.05               # 5 %
ROW_DROP_THRESHOLD: float  = 0.20               # 20 %
YEAR_MIN: int = 1960
# Absolute path so the file is always written to the project's logs/ directory,
# regardless of the CWD when spark-submit launches the driver.
_PROJECT_ROOT     = Path(__file__).parent.parent
RUN_METADATA_PATH = str(_PROJECT_ROOT / "logs" / "run_metadata.json")
 
# Severity constants
CRITICAL = "CRITICAL"
WARNING  = "WARNING"
 
# Status constants
PASS = "PASS"
FAIL = "FAIL"
WARN = "WARN"
SKIP = "SKIP"


class WorldBankSparkJob:
    """PySpark job for processing World Bank GDP data."""

    def __init__(self, config_path: str = None):
        """
        Initialize the Spark job.
        
        Args:
            config_path: Path to configuration file (optional, auto-resolved if None)
        """

        self.config = load_config(config_path)
        self.logger = setup_logging(self.config)
        self.spark = self.create_spark_session()

    # Helpers

    @staticmethod
    def convert_to_s3a(s3_path: str) -> str:
        """
        Convert s3:// URI to s3a:// for Spark compatibility.
        
        Args:
            s3_path: S3 path with s3:// protocol
            
        Returns:
            S3 path with s3a:// protocol
        """
        return s3_path.replace("s3://", "s3a://")

    def _postgres_url(self) -> str:
        pg = self.config["postgres"]

        return f"jdbc:postgresql://{pg['host']}:{pg.get('port', 5432)}/{pg['database']}"
    

    def _postgres_props(self) -> dict:
        pg = self.config["postgres"]
        sslmode = pg.get("sslmode", "prefer")

        return {
            "user":     pg["user"],
            "password": pg["password"],
            "driver":   "org.postgresql.Driver",
            "ssl":      "true" if sslmode != "disable" else "false",
            "sslmode":  sslmode,
        }

    def _get_pg_connection(self):
        """Return a psycopg2 connection using config values."""
        pg = self.config["postgres"]
        return psycopg2.connect(
            host=pg["host"],
            port=pg.get("port", 5432),
            dbname=pg["database"],
            user=pg["user"],
            password=pg["password"],
            sslmode=pg.get("sslmode", "prefer"),
        )
    

    # Spark Session

    def create_spark_session(self) -> SparkSession:
        """
        Create and configure Spark session for S3 access.
        
        Returns:
            Configured SparkSession
        """
        try:
            spark_cfg  = self.config.get("spark", {})
            jdbc_jar   = spark_cfg.get("jdbc_driver_jar", "")

            spark = (SparkSession.builder
                    .appName(spark_cfg.get("app_name", "WorldBankPipeline"))
                    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                        "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
                    .config("spark.driver.extraClassPath",   jdbc_jar)
                    .config("spark.executor.extraClassPath", jdbc_jar)
                    .config("spark.sql.adaptive.enabled", "true")
                    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                    .config("spark.hadoop.fs.s3a.multipart.size", "67108864")
                    .config("spark.hadoop.fs.s3a.fast.upload", "true")
                    .config("spark.hadoop.fs.s3a.fast.upload.buffer", "bytebuffer")
                    .config("spark.driver.memory", "2g")
                    .config("spark.executor.memory", "2g")
                    .config("spark.driver.maxResultSize", "1g")
                    # FileOutputCommitter v2 — each task moves its output immediately
                    # after completing, avoiding the bulk rename at job commit that
                    # causes NoSuchKey errors on S3's non-atomic rename simulation
                    .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
                    .getOrCreate())
            
            self.logger.info("SparkSession created successfully")
            self.logger.info(f"Spark version: {spark.version}")
            return spark
            
        except Exception as e:
            self.logger.error(f"Failed to create SparkSession: {e}")
            raise

    # Extract / Transform

    def read_raw_data(self) -> DataFrame:
        """
        Read raw JSON data from S3.
        
        Returns:
            DataFrame containing raw data
        """
        base_path = get_s3_path(self.config, "raw")
        raw_path = f"{base_path}/year=*/month=*/day=*/data.json"

        # Convert to s3a:// protocol for Spark
        raw_path = self.convert_to_s3a(raw_path)

        try:
            self.logger.info(f"Reading raw data from: {raw_path}")

            df = (self.spark.read.option('multiline', 'true').json(raw_path))

            row_count = df.count()
            self.logger.info(f"Successfully read {row_count:,} rows from raw data")
            self.logger.info(f"Schema: {df.schema.simpleString()}")

            return df
        
        except Exception as e:
            self.logger.error(f"Failed to read raw data: {e}")
            raise

    def clean_data(self, df: DataFrame) -> DataFrame:
        """
        Clean and transform raw data.
        
        Args:
            df: Raw DataFrame
            
        Returns:
            Cleaned DataFrame
        """
        try:
            self.logger.info('Starting data cleaning...')

            # Select, rename, cast, and drop null GDP rows in one pass.
            # Null-count detail is captured by validate_data_quality(); no need
            # to trigger a second full scan here just for a log line.
            cleaned_df = (
                df.select(
                    col('country.value').alias('country_name'),
                    col('country.id').alias('country_iso3'),
                    col('indicator.id').alias('indicator_id'),
                    col('date').cast(IntegerType()).alias('year'),
                    col('value').cast(DoubleType()).alias('gdp_usd')
                )
                .filter(col('gdp_usd').isNotNull())
            )

            row_count = cleaned_df.count()

            self.logger.info("Data cleaning complete:")
            self.logger.info(f"  - Rows retained (non-null gdp_usd): {row_count:,}")
            self.logger.info("Sample of cleaned data:")
            cleaned_df.show(5, truncate=False)

            return cleaned_df
        
        except Exception as e:
            self.logger.error(f"Failed to clean data: {e}")
            raise

    # Data Quality Validation

    def validate_data_quality(self, df: DataFrame, prev_run_count: int = None) -> Dict[str, Any]:
        """
        Run a suite of data quality checks on the cleaned DataFrame and return
        a structured report.
 
        Checks
        ------
        1. **Null counts per column** (CRITICAL)
           Fails if the ``gdp_usd`` null rate exceeds 5 %.
        2. **GDP value range** (WARNING)
           Flags values below $1 M or above $30 T as suspicious.
        3. **Duplicate detection** (WARNING)
           Detects duplicate ``(country_iso3, year)`` combinations.
        4. **Row count vs previous run** (WARNING)
           Warns if the total row count drops by more than 20 % compared with
           the previous run (persisted in ``logs/run_metadata.json``).
        5. **Year range validation** (CRITICAL)
           Fails if any row contains a year outside [1960, current year].
 
        Parameters
        ----------
        df : DataFrame
            Cleaned DataFrame produced by :meth:`clean_data`.
        prev_run_count : int, optional
            Override the persisted previous run count for comparison.
            When ``None`` the value is read from ``logs/run_metadata.json``.
 
        Returns
        -------
        dict
            Data quality report with the following top-level keys:
 
            * ``timestamp``      - ISO-8601 string of when validation ran.
            * ``total_rows``     - Row count of the validated DataFrame.
            * ``checks``         - Dict of individual check results (see below).
            * ``overall_status`` - ``"PASS"``, ``"WARN"``, or ``"FAIL"``.
 
            Each check result contains at minimum:
 
            * ``status``   - ``"PASS"`` | ``"WARN"`` | ``"FAIL"`` | ``"SKIP"``
            * ``severity`` - ``"CRITICAL"`` | ``"WARNING"``
            * ``message``  - Human-readable summary.
 
        Raises
        ------
        DataQualityError
            If any CRITICAL check fails (null rate or year range).
 
        Notes
        -----
        * All Spark aggregations that can be batched are combined into a single
          ``.agg()`` call to minimise shuffles and Spark actions.
        * The DataFrame is cached at the start and unpersisted before returning.
        * The current run's row count is always written back to
          ``logs/run_metadata.json`` for the next run's comparison.
        """

        self.logger.info("=" * 60)
        self.logger.info("Running Data Quality Validation")
        self.logger.info("=" * 60)

        current_year = datetime.now().year

        report: Dict[str, Any] = {
            "timestamp": datetime.now().isoformat(),
            "total_rows": 0,
            "checks": {},
            "overall_status": "PASS"
        }

        critical_failures: list = []

        # Cache df so all checks use the same in-memory copy
        df.cache()
        total_rows = df.count()
        report["total_rows"] = total_rows

        self.logger.info(f"Total rows to validate: {total_rows:,}")

        # Check Null counts per column
        columns = ["country_name", "country_iso3", "indicator_id", "year", "gdp_usd"]

        null_agg_exprs = [
            spark_sum(when(col(c).isNull(), 1).otherwise(0)).alias(f"null_{c}") for c in columns
        ]
        null_row = df.agg(*null_agg_exprs).collect()[0]
        null_counts = {c: int(null_row[f"null_{c}"]) for c in columns}

        gdp_null_count = null_counts['gdp_usd']
        gdp_null_rate = gdp_null_count / total_rows if total_rows > 0 else 0.0
        null_passed = gdp_null_rate <= NULL_RATE_THRESHOLD

        null_check = {
            "status": PASS if null_passed else FAIL,
            "severity": CRITICAL,
            "null_counts": null_counts,
            "null_rates": {c: round(null_counts[c] / total_rows, 6) if total_rows > 0 else 0.0 for c in columns},
            "gdp_usd_null_rate": round(gdp_null_rate, 6),
            "threshold": NULL_RATE_THRESHOLD,
            "message": (
                f"gdp_usd null rate {gdp_null_rate:.2%} is within the "
                f"{NULL_RATE_THRESHOLD:.0%} threshold"
                if null_passed else
                f"CRITICAL: gdp_usd null rate {gdp_null_rate:.2%} exceeds "
                f"the {NULL_RATE_THRESHOLD:.0%} threshold "
                f"({gdp_null_count:,} of {total_rows:,} rows)"
            )
        }

        report["checks"]["null_counts"] = null_check
        self.logger.info(f"[NULL CHECK] {null_check['message']}")
        for col_name, cnt in null_counts.items():
            rate = cnt / total_rows if total_rows > 0 else 0.0
            self.logger.info(f"{col_name:<20} {cnt:>8,} nulls ({rate:.2%})")
        
        if not null_passed:
            critical_failures.append("null_counts")

        # Check GDP values range
        range_row = df.agg(
            spark_min(when(col("gdp_usd").isNotNull(), col("gdp_usd"))).alias("min_gdp"),
            spark_max(when(col("gdp_usd").isNotNull(), col("gdp_usd"))).alias("max_gdp"),
            spark_sum(
                when(col("gdp_usd").isNotNull() & (col("gdp_usd") < GDP_MIN_SUSPICIOUS), 1)
                .otherwise(0)
            ).alias("below_min"),
            spark_sum(
                when(col("gdp_usd").isNotNull() & (col("gdp_usd") > GDP_MAX_SUSPICIOUS), 1)
                .otherwise(0)
            ).alias("above_max"),
            spark_sum(when(col("gdp_usd").isNotNull(), 1).otherwise(0)).alias("non_null_count")
        ).collect()[0]

        below_min = int(range_row["below_min"])
        above_max = int(range_row["above_max"])
        suspicious = below_min + above_max
        non_null_count = int(range_row["non_null_count"])
        suspicious_rate = suspicious / non_null_count if non_null_count > 0 else 0.0
        range_passed = suspicious == 0

        range_check = {
            "status": PASS if range_passed else WARN,
            "severity": WARNING,
            "min_gdp_usd": float(range_row["min_gdp"]) if range_row["min_gdp"] is not None else None,
            "max_gdp_usd": float(range_row["max_gdp"]) if range_row["max_gdp"] is not None else None,
            "suspicious_below_1m": below_min,
            "suspicious_above_30t": above_max,
            "suspicious_total": suspicious,
            "suspicious_rate": round(suspicious_rate, 6),
            "thresholds": {"min_usd": GDP_MIN_SUSPICIOUS, "max_usd": GDP_MAX_SUSPICIOUS},
            "message": (
                "All GDP values are within the expected range"
                if range_passed else
                f"WARNING: {suspicious:,} suspicious GDP values detected — "
                f"{below_min:,} below $1 M, {above_max:,} above $30 T "
                f"({suspicious_rate:.2%} of non-null rows)"
            )
        }
        report["checks"]["gdp_value_range"] = range_check
        self.logger.info(f"[GDP RANGE] {range_check['message']}")
        if range_row['min_gdp'] is not None:
            self.logger.info(f"Min GDP: ${range_row['min_gdp']:>20,.2f} | Max GDP: ${range_row['max_gdp']:>20,.2f}")

        # Check Duplicate (country_iso3, year) combinations

        keyed_df = df.filter(col("country_iso3").isNotNull() & col("year").isNotNull())
        total_keyed = keyed_df.count()
        distinct_keys = keyed_df.select("country_iso3", "year").distinct().count()
        duplicate_count = total_keyed - distinct_keys
        duplicate_rate = duplicate_count / total_keyed if total_keyed > 0 else 0.0
        dup_passed = duplicate_count == 0

        dup_check = {
            "status":          PASS if dup_passed else WARN,
            "severity":        WARNING,
            "total_key_rows":  total_keyed,
            "distinct_keys":   distinct_keys,
            "duplicate_count": duplicate_count,
            "duplicate_rate":  round(duplicate_rate, 6),
            "message": (
                "No duplicate (country_iso3, year) combinations found"
                if dup_passed else
                f"WARNING: {duplicate_count:,} duplicate (country_iso3, year) "
                f"rows detected ({duplicate_rate:.2%} of keyed rows)"
            )
        }
        report["checks"]["duplicates"] = dup_check
        self.logger.info(f"[DUPLICATES]{dup_check['message']}")

        # Check Row Count vs Previous Run
        resolved_prev = prev_run_count
        if resolved_prev is None:
            try:
                if os.path.exists(RUN_METADATA_PATH):
                    with open(RUN_METADATA_PATH, 'r') as fh:
                        meta = json.load(fh)
                        resolved_prev = meta.get("last_run_row_count")

            except Exception as e:
                self.logger.warning(f"Could not read previous run metadata: {e}")

        if resolved_prev is not None and resolved_prev > 0:
            row_delta = resolved_prev - total_rows
            row_drop_rate = row_delta / resolved_prev
            row_count_passed = row_drop_rate <= ROW_DROP_THRESHOLD
            row_count_check = {
                "status": PASS if row_count_passed else WARN,
                "severity": WARNING,
                "current_count": total_rows,
                "previous_count": resolved_prev,
                "row_delta": row_delta,
                "row_drop_rate": round(row_drop_rate, 6),
                "threshold": ROW_DROP_THRESHOLD,
                "message": (
                    f"Row count change within threshold — "
                    f"prev: {resolved_prev:,}  curr: {total_rows:,}  "
                    f"delta: {row_delta:+,} ({row_drop_rate:+.2%})"
                    if row_count_passed else
                    f"WARNING: Row count dropped {row_drop_rate:.2%} "
                    f"(from {resolved_prev:,} to {total_rows:,}); "
                    f"threshold is {ROW_DROP_THRESHOLD:.0%}"
                )
            }
        else:
            row_count_check = {
                "status": SKIP,
                "severity": WARNING,
                "current_count": total_rows,
                "previous_count": None,
                "message": "No previous run data found - skipping row count comparison."
            }

        report["checks"]["row_count_comparison"] = row_count_check
        self.logger.info(f"[ROW COUNT] {row_count_check['message']}")

        # Persist current run count for the next execution
        try:
            os.makedirs("logs", exist_ok=True)
            existing_meta: dict = {}
            if os.path.exists(RUN_METADATA_PATH):
                with open(RUN_METADATA_PATH, 'r') as fh:
                    existing_meta = json.load(fh)
            existing_meta['last_run_row_count'] = total_rows
            existing_meta['last_run_timestamp'] = datetime.now().isoformat()
            with open(RUN_METADATA_PATH, 'w') as fh:
                json.dump(existing_meta, fh, indent=2)

        except Exception as e:
            self.logger.warning(f"Could not persist run metadata: {e}")


        # Check year range validation
        year_row = df.filter(col('year').isNotNull()).agg(
            spark_min('year').alias('min_year'),
            spark_max('year').alias('max_year'),
            spark_sum(
                when((col('year') < YEAR_MIN) | (col('year') > current_year), 1).otherwise(0)
            ).alias('out_of_range')
        ).collect()[0]

        out_of_range_count = int(year_row['out_of_range'])
        out_of_range_rate = out_of_range_count / total_rows if total_rows > 0 else 0.0
        year_passed = out_of_range_count == 0

        year_check = {
            "status": PASS if year_passed else FAIL,
            "severity": CRITICAL,
            "min_year_found": int(year_row["min_year"]) if year_row["min_year"] is not None else None,
            "max_year_found": int(year_row["max_year"]) if year_row["max_year"] is not None else None,
            "valid_range": [YEAR_MIN, current_year],
            "out_of_range_count": out_of_range_count,
            "out_of_range_rate": round(out_of_range_rate, 6),
            "message": (
                f"All years are within the valid range [{YEAR_MIN}–{current_year}]"
                if year_passed else
                f"CRITICAL: {out_of_range_count:,} rows ({out_of_range_rate:.2%}) "
                f"contain a year outside [{YEAR_MIN}–{current_year}]"
            ),
        }

        report["checks"]['year_range'] = year_check
        self.logger.info(f"[YEAR RANGE] {year_check['message']}")
        if year_row['min_year'] is not None:
            self.logger.info(f"Year range found: {year_row['min_year']} - {year_row['max_year']}")
        
        if not year_passed:
            critical_failures.append("year_range")


        # Summary

        has_warnings = any(c['status'] == WARN for c in report['checks'].values())

        if critical_failures:
            report['overall_status'] = FAIL
        elif has_warnings:
            report["overall_status"] = WARN

        _icons = {PASS: "✓", WARN: "⚠", FAIL: "✗", SKIP: "-"}
        self.logger.info("-" * 60)
        self.logger.info("DATA QUALITY REPORT SUMMARY")
        self.logger.info('-' * 60)
        for check_name, check_result in report["checks"].items():
            icon = _icons.get(check_result['status'], '?')
            self.logger.info(f" {icon}  {check_name:<26}    [{check_result['status']:<4}]   severity={check_result['severity']}")
        
        self.logger.info(f"OVERALL STATUS: {report['overall_status']}")
        self.logger.info('-' * 60)

        df.unpersist()

        if critical_failures:
            raise DataQualityError(
                message=(
                    f"Data quality validation FAILED. "
                    f"Critical checks failed: {', '.join(critical_failures)}"
                ),
                failed_checks=critical_failures,
                report=report
            )
        
        return report


    def write_processed_data(self, df: DataFrame) -> None:
        """
        Write cleaned data to S3 as Parquet, partitioned by year.
        
        Args:
            df: Cleaned DataFrame
        """
        date_partition = datetime.now().strftime("%Y/%m/%d")
        processed_path = get_s3_path(self.config, 'processed', date_partition=date_partition)
        processed_path = self.convert_to_s3a(processed_path)

        try:
            self.logger.info(f"Writing processed data to: {processed_path}")

            df.write.mode('overwrite').parquet(processed_path)

            self.logger.info(f"Successfully wrote processed data (partitioned by year)")

        except Exception as e:
            self.logger.error(f"Failed to write processed data: {e}")
            raise


    def create_analytics(self, df: DataFrame) -> DataFrame:
        """
        Create analytics aggregation: average GDP per country.
        
        Args:
            df: Cleaned DataFrame
            
        Returns:
            Analytics DataFrame
        """
        try:
            self.logger.info('Creating analytics layer...')

            analytics_df = (df.groupBy('country_name', 'country_iso3')
                            .agg(
                                avg('gdp_usd').alias('avg_gdp_usd'),
                                count('gdp_usd').alias('year_count')
                                )
                                .orderBy(col('avg_gdp_usd').desc()))
            
            analytics_df.cache()
            
            row_count = analytics_df.count()
            self.logger.info("Analytics aggregation complete:")
            self.logger.info(f"  - Countries analyzed: {row_count:,}")

            # Show top 10 countries by average GDP
            self.logger.info("Top 10 countries by average GDP:")
            analytics_df.show(10, truncate=False)

            return analytics_df
        
        except Exception as e:
            self.logger.error(f"Failed to create analytics: {e}")
            raise


    def write_analytics_data(self, df: DataFrame) -> None:
        """
        Write analytics data to S3 as Parquet.
        
        Args:
            df: Analytics DataFrame
        """
        date_partition = datetime.now().strftime("%Y/%m/%d")
        analytics_path = get_s3_path(self.config, 'analytics', date_partition=date_partition)
        analytics_path = self.convert_to_s3a(analytics_path)

        try:
            self.logger.info(f"Writing analytics data to: {analytics_path}")

            df.write.mode('overwrite').parquet(analytics_path)

            self.logger.info('Successfully wrote analytics data')

        except Exception as e:
            self.logger.error(f"Failed to write analytics data: {e}")
            raise



    def write_to_staging(self, df: DataFrame) -> None:
        """
        Bulk-load the cleaned DataFrame into stg_gdp via JDBC.

        Uses SaveMode.Overwrite with truncate=true so the TRUNCATE and all
        INSERTs are issued through the same JDBC session — eliminating the
        two-connection race condition that would leave stg_gdp empty if a
        separate psycopg2 TRUNCATE committed before a failing JDBC write.

        The staging table mirrors the cleaned DataFrame schema:
            country_name, country_iso3, indicator_id, year, gdp_usd

        Parameters
        ----------
        df : DataFrame
            Cleaned DataFrame produced by clean_data().
        """
        try:
            row_count = df.count()
            self.logger.info(f"Writing {row_count:,} rows to stg_gdp...")

            url   = self._postgres_url()
            props = self._postgres_props()

            # mode("overwrite") + truncate=true: Spark issues TRUNCATE TABLE
            # through the same JDBC connection before batching INSERTs.
            # This preserves indexes/constraints (unlike DROP+CREATE) and
            # avoids the committed-truncate / failed-write window.
            df.write \
                .mode("overwrite") \
                .option("truncate", "true") \
                .option("batchsize", "5000") \
                .jdbc(url=url, table="stg_gdp", properties=props)

            self.logger.info("Staging load complete")

        except Exception as e:
            self.logger.error(f"Failed to write to staging data: {e}")
            raise


    def run_sql_pipeline(self) -> None:
        """
        Execute sql/load_pipeline.sql against PostgreSQL.
 
        That script:
          1. Populates dim_country  (INSERT … SELECT DISTINCT … ON CONFLICT)
          2. Populates dim_indicator (INSERT … SELECT DISTINCT … ON CONFLICT)
          3. Populates dim_time      (INSERT … SELECT DISTINCT … ON CONFLICT)
          4. Loads fact_gdp_measurements from staging (ON CONFLICT DO UPDATE)
          5. Truncates stg_gdp
 
        Everything runs inside a single transaction so a failure rolls back
        cleanly and leaves the star schema untouched.
        """
        sql_path = Path(__file__).parent.parent / "sql" / "load_pipeline.sql"

        try:
            self.logger.info("Reading SQL pipeline from: %s", sql_path)
            sql = sql_path.read_text()

            conn = self._get_pg_connection()
            try:
                cursor = conn.cursor()
                self.logger.info("Executing SQL pipeline (staging -> dims -> facts)...")
                cursor.execute(sql)
                conn.commit()
                cursor.close()
            finally:
                conn.close()
            self.logger.info("SQL pipeline executed successfully")

        except Exception as e:
            self.logger.error(f"SQL pipeline execution failed: {e}")
            raise
    

    def run(self) -> None:
        """Execute the complete pipeline"""
        try:
            self.logger.info("="*80)
            self.logger.info("Starting World Bank GDP Data Pipeline - PySpark Job")
            self.logger.info("="*80)

            # Step 1: Read raw data
            raw_df = self.read_raw_data()

            # Step 2: Clean raw data
            cleaned_df = self.clean_data(raw_df)

            # Step 3: Data quality validation (caches internally, then unpersists)
            dq_report = self.validate_data_quality(cleaned_df)
            self.logger.info(f"Data quality overall status: {dq_report['overall_status']}")

            # Cache after validation so subsequent steps reuse the same data
            cleaned_df.cache()

            # Step 4: Write processed/cleaned data
            self.write_processed_data(cleaned_df)

            # Step 5: Create analytics data
            analytics_df = self.create_analytics(cleaned_df)
            # Guarantee unpersist even if the write step raises.
            try:
                # Step 6: Write analytics data
                self.write_analytics_data(analytics_df)
            finally:
                analytics_df.unpersist()

            self.write_to_staging(cleaned_df)
            cleaned_df.unpersist()

            self.run_sql_pipeline()

            self.logger.info("="*80)
            self.logger.info("Pipeline completed successfully")
            self.logger.info("="*80)

        except DataQualityError as dqe:
            self.logger.error(f"Pipeline aborted - data quality failure {dqe}")
            self.logger.error(f"Failed checks: {dqe.failed_checks}")
            raise

        except Exception as e:
            self.logger.error(f"Pipeline failed: {e}")
            raise

        finally:
            self.spark.stop()
            self.logger.info('SparkSession stopped')


def main():
    """Main entry point for the Spark job."""
    try:
        job = WorldBankSparkJob()
        job.run()
        sys.exit(0)

    except Exception as e:
        logging.error(f"Job failed with error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()