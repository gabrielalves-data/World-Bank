import logging
import sys
from typing import Dict, Any
from pathlib import Path
import yaml
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, avg, count
from pyspark.sql.types import IntegerType, DoubleType
from datetime import datetime
import psycopg2

from utils.config_loader import load_config
from utils.logging_config import setup_logging
from scripts.s3_upload import get_s3_path


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

        return {
            "user": pg["user"],
            "password": pg["password"],
            "driver": "org.postgresql.Driver"
        }
    
    def _get_pg_connection(self):
        """Return a psycopg2 connection using config values."""
        pg = self.config["postgres"]
        return psycopg2.connect(
            host=pg["host"],
            port=pg.get("port", 5432),
            dbname=pg["database"],
            user=pg["user"],
            password=pg["password"]
        )
    

    def create_spark_session(self) -> SparkSession:
        """
        Create and configure Spark session for S3 access.
        
        Returns:
            Configured SparkSession
        """
        try:
            spark = (SparkSession.builder
                    .appName('WorldBankPipeline')
                    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                        "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
                    .config("spark.driver.extraClassPath",
                        "/opt/spark-3.5.0-bin-hadoop3/jars/postgresql-42.7.1.jar")
                    .config("spark.executor.extraClassPath",
                        "/opt/spark-3.5.0-bin-hadoop3/jars/postgresql-42.7.1.jar")
                    .config("spark.sql.adaptive.enabled", "true")
                    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                    .config("spark.hadoop.fs.s3a.multipart.size", "67108864")
                    .config("spark.hadoop.fs.s3a.fast.upload", "true")
                    .config("spark.hadoop.fs.s3a.fast.upload.buffer", "bytebuffer")
                    .config("spark.driver.memory", "2g")
                    .config("spark.executor.memory", "2g")
                    .config("spark.driver.maxResultSize", "1g")
                    .getOrCreate())
            
            self.logger.info("SparkSession created successfully")
            self.logger.info(f"Spark version: {spark.version}")
            return spark
            
        except Exception as e:
            self.logger.error(f"Failed to create SparkSession: {e}")
            raise

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
            self.logger.info(f"Schema: {df.printSchema()}")

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

            # Select and rename columns with proper types
            cleaned_df = df.select(
                col('country.value').alias('country_name'),
                col('country.id').alias('country_iso3'),
                col('indicator.id').alias('indicator_id'),
                col('date').cast(IntegerType()).alias('year'),
                col('value').cast(DoubleType()).alias('gdp_usd')
            )

            # Drop rows with null GDP values
            cleaned_df = cleaned_df.filter(col('gdp_usd').isNotNull())

            row_count = cleaned_df.count()
            null_count = df.count() - row_count

            self.logger.info("Data cleaning complete:")
            self.logger.info(f"  - Rows after cleaning: {row_count:,}")
            self.logger.info(f"  - Rows with null GDP removed: {null_count:,}")

            self.logger.info("Sample of cleaned data:")
            cleaned_df.show(5, truncate=False)

            return cleaned_df
        
        except Exception as e:
            self.logger.error(f"Failed to clean data: {e}")
            raise


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
        Truncate stg_gdp and bulk-load the cleaned DataFrame into it via JDBC.
 
        The staging table mirrors the cleaned DataFrame schema:
            country_name, country_iso3, indicator_id, year, gdp_usd
 
        Parameters
        ----------
        df : DataFrame
            Cleaned DataFrame produced by clean_data().
        """
        try:
            # Truncate before load so a re-run never leaves stale data
            self.logger.info("Truncating stg_gdp...")
            conn = self._get_pg_connection()
            cursor = conn.cursor()
            cursor.execute("TRUNCATE TABLE stg_gdp;")
            conn.commit()
            cursor.close()
            conn.close()
            self.logger.info("stg_gdp truncated successfully")

            row_count = df.count()
            self.logger.info(f"Writing {row_count:,} rows to stg_gdp...")

            url = self._postgres_url()
            props = {
                "user": self.config["postgres"]["user"],
                "password": self.config["postgres"]["password"],
                "driver": "org.postgresql.Driver"
            }

            # Bulk insert via JDBC
            df.write.mode("append").option("batchsize", "5000").jdbc(url=url, table="stg_gdp", properties=props)
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
            cursor = conn.cursor()

            self.logger.info("Executing SQL pipeline (staging -> dims -> facts)...")
            cursor.execute(sql)
            conn.commit()

            cursor.close()
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

            cleaned_df.cache()

            # Step 3: Write processed/cleaned data
            self.write_processed_data(cleaned_df)

            # Step 4: Create analytics data
            analytics_df = self.create_analytics(cleaned_df)

            # Step 5: Write analytics data
            self.write_analytics_data(analytics_df)

            analytics_df.unpersist()

            self.write_to_staging(cleaned_df)
            cleaned_df.unpersist()

            self.run_sql_pipeline()

            self.logger.info("="*80)
            self.logger.info("Pipeline completed successfully")
            self.logger.info("="*80)

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