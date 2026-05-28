import logging
import os
import sys
import pytest
import requests

from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType
)

from scripts.extract import fetch_page, fetch_all_records

os.environ["PYSPARK_PYTHON"]        = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


# ──────────────────────────────────────────────────────────────────────────────
# Helpers / shared fixtures
# ──────────────────────────────────────────────────────────────────────────────

def _make_response(json_data: list, status_code: int = 200) -> MagicMock:
    """Build a mock requests.Response."""
    mock_resp = MagicMock()
    mock_resp.status_code = status_code
    mock_resp.json.return_value = json_data
    if status_code >= 400:
        mock_resp.raise_for_status.side_effect = requests.HTTPError(
            response=mock_resp
        )
    else:
        mock_resp.raise_for_status.return_value = None
    return mock_resp


# ──────────────────────────────────────────────────────────────────────────────
# Fixtures
# ──────────────────────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def spark():
    """
    Single SparkSession shared across all Spark tests in this module.
    Uses local[1] to keep it fast and single-threaded.

    Extra configs here are all aimed at Windows reliability:
      - spark.python.worker.reuse     : avoids repeated worker spawning
      - spark.executor.heartbeatInterval / network.timeout: prevents
        premature timeouts while the Python worker boots on slow Windows FS
    """
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("test_worldbank")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.python.worker.reuse", "true")
        .config("spark.executor.heartbeatInterval", "60s")
        .config("spark.network.timeout", "300s")
        .config("spark.sql.execution.pyspark.udf.simplifiedTraceback.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture()
def logger():
    return logging.getLogger("test")


@pytest.fixture()
def api_config():
    return {
        "api": {
            "base_url": "http://api.worldbank.org/v2",
            "indicator": "NY.GDP.MKTP.CD",
            "per_page": 1000,
            "format": "json",
        }
    }


# ──────────────────────────────────────────────────────────────────────────────
# fetch_page() tests
# ──────────────────────────────────────────────────────────────────────────────

class TestFetchPage:

    BASE_URL  = "http://api.worldbank.org/v2"
    INDICATOR = "NY.GDP.MKTP.CD"

    def test_successful_response_returns_metadata_and_records(self, monkeypatch):
        """Happy-path: returns (metadata dict, list of records)."""
        metadata = {"page": 1, "pages": 3, "per_page": 2, "total": 6}
        records  = [{"country": {"id": "US"}, "value": 25e12}]
        mock_response = _make_response([metadata, records])

        with patch("scripts.extract.requests.get", return_value=mock_response) as mock_get:
            result_meta, result_records = fetch_page(
                self.BASE_URL, self.INDICATOR, page=1, per_page=2, fmt="json"
            )

        assert result_meta    == metadata
        assert result_records == records

        expected_url = f"{self.BASE_URL}/country/all/indicator/{self.INDICATOR}"
        mock_get.assert_called_once_with(
            expected_url,
            params={"format": "json", "per_page": 2, "page": 1},
            timeout=60,
        )

    def test_none_records_returns_empty_list(self):
        """When data[1] is None the function should return [] instead of None."""
        metadata = {"page": 1, "pages": 1, "per_page": 1000, "total": 0}

        with patch("scripts.extract.requests.get", return_value=_make_response([metadata, None])):
            _, result_records = fetch_page(
                self.BASE_URL, self.INDICATOR, page=1, per_page=1000, fmt="json"
            )

        assert result_records == []
        assert isinstance(result_records, list)

    def test_http_error_raises_exception(self):
        """A 4xx/5xx response should propagate as requests.HTTPError."""
        with patch("scripts.extract.requests.get", return_value=_make_response([], status_code=404)):
            with pytest.raises(requests.HTTPError):
                fetch_page(
                    self.BASE_URL, self.INDICATOR, page=1, per_page=1000, fmt="json"
                )

    def test_pagination_params_forwarded_correctly(self):
        """page and per_page should be passed verbatim to requests.get."""
        metadata = {"page": 7, "pages": 10, "per_page": 500, "total": 5000}
        records  = [{"value": 1}]

        with patch("scripts.extract.requests.get", return_value=_make_response([metadata, records])) as mock_get:
            fetch_page(self.BASE_URL, self.INDICATOR, page=7, per_page=500, fmt="json")

        assert mock_get.call_args[1]["params"]["page"]     == 7
        assert mock_get.call_args[1]["params"]["per_page"] == 500


# ──────────────────────────────────────────────────────────────────────────────
# fetch_all_records() tests
# ──────────────────────────────────────────────────────────────────────────────

class TestFetchAllRecords:

    def test_single_page_returns_all_records(self, api_config, logger):
        """When total_pages == 1, fetch_page is called exactly once."""
        records = [{"id": "A"}, {"id": "B"}]

        with patch("scripts.extract.fetch_page", return_value=({"pages": 1, "total": 2}, records)):
            result = fetch_all_records(api_config, logger)

        assert result == records

    def test_multiple_pages_aggregated(self, api_config, logger):
        """Records from all pages should be concatenated in order."""
        page1_records = [{"id": "A"}, {"id": "B"}]
        page2_records = [{"id": "C"}, {"id": "D"}]
        page3_records = [{"id": "E"}]

        side_effects = [
            ({"pages": 3, "total": 5}, page1_records),
            ({}, page2_records),
            ({}, page3_records),
        ]

        with patch("scripts.extract.fetch_page", side_effect=side_effects) as mock_fp:
            result = fetch_all_records(api_config, logger)

        assert result == page1_records + page2_records + page3_records
        assert mock_fp.call_count == 3

    def test_correct_pages_requested(self, api_config, logger):
        """fetch_page should be called for pages 1, 2, 3 in sequence."""
        side_effects = [
            ({"pages": 3, "total": 6}, [{"id": str(i)} for i in range(2)]),
            ({}, [{"id": str(i)} for i in range(2, 4)]),
            ({}, [{"id": str(i)} for i in range(4, 6)]),
        ]

        with patch("scripts.extract.fetch_page", side_effect=side_effects) as mock_fp:
            fetch_all_records(api_config, logger)

        # Page 1 is passed as a kwarg (page=1) but pages 2+ are passed
        # positionally in the for-loop inside fetch_all_records, so we must
        # handle both: kwargs["page"] for call 1, args[2] for calls 2+.
        def _get_page(c):
            return c.kwargs["page"] if "page" in c.kwargs else c.args[2]

        pages_requested = [_get_page(c) for c in mock_fp.call_args_list]
        assert pages_requested == [1, 2, 3]

    def test_empty_api_response_returns_empty_list(self, api_config, logger):
        """When total_pages is 1 and records are empty, result should be []."""
        with patch("scripts.extract.fetch_page", return_value=({"pages": 1, "total": 0}, [])):
            result = fetch_all_records(api_config, logger)

        assert result == []

    def test_config_values_passed_to_fetch_page(self, api_config, logger):
        """Base URL, indicator, per_page, and format from config are forwarded."""
        with patch("scripts.extract.fetch_page", return_value=({"pages": 1, "total": 0}, [])) as mock_fp:
            fetch_all_records(api_config, logger)

        mock_fp.assert_called_once_with(
            api_config["api"]["base_url"],
            api_config["api"]["indicator"],
            page=1,
            per_page=api_config["api"]["per_page"],
            fmt=api_config["api"]["format"],
        )


# ──────────────────────────────────────────────────────────────────────────────
# WorldBankSparkJob.clean_data() tests
# ──────────────────────────────────────────────────────────────────────────────

class TestCleanData:
    """
    Tests for WorldBankSparkJob.clean_data().

    We instantiate a minimal stub of WorldBankSparkJob so the Spark-heavy
    __init__ (S3 config, full SparkSession setup) is bypassed entirely.
    clean_data() only needs self.logger and the Spark session is used only
    to build input DataFrames in the tests.
    """

    # ------------------------------------------------------------------
    # Raw-data schema that mimics the nested JSON from the World Bank API
    # after Spark reads it (country / indicator are structs).
    # ------------------------------------------------------------------
    RAW_SCHEMA = StructType.fromJson({
        "type": "struct",
        "fields": [
            {"name": "country",   "type": {"type": "struct", "fields": [
                {"name": "id",    "type": "string", "nullable": True, "metadata": {}},
                {"name": "value", "type": "string", "nullable": True, "metadata": {}},
            ]}, "nullable": True, "metadata": {}},
            {"name": "indicator", "type": {"type": "struct", "fields": [
                {"name": "id",    "type": "string", "nullable": True, "metadata": {}},
            ]}, "nullable": True, "metadata": {}},
            {"name": "date",  "type": "string",  "nullable": True, "metadata": {}},
            {"name": "value", "type": "string",  "nullable": True, "metadata": {}},
        ]
    })

    @pytest.fixture(autouse=True)
    def job(self, spark):
        """
        Lightweight stub: skips __init__ and injects only what clean_data needs.
        """
        from scripts.spark_job import WorldBankSparkJob

        instance = object.__new__(WorldBankSparkJob)
        instance.logger = logging.getLogger("test_spark")
        instance.spark  = spark
        return instance

    # Fully nested schema mirroring what Spark produces when reading the
    # World Bank JSON from S3.  Declared here so createDataFrame NEVER
    # inspects data values for type inference — safe even when every
    # 'value' field is None (the all-null GDP test case).
    _RAW_SCHEMA = StructType([
        StructField("country", StructType([
            StructField("id",    StringType(), nullable=True),
            StructField("value", StringType(), nullable=True),
        ]), nullable=True),
        StructField("indicator", StructType([
            StructField("id", StringType(), nullable=True),
        ]), nullable=True),
        StructField("date",  StringType(), nullable=True),
        StructField("value", StringType(), nullable=True),
    ])

    def _make_raw_df(self, spark, rows: list):
        """
        Build a raw DataFrame that mirrors the nested struct layout Spark
        produces when reading the World Bank JSON directly from S3.

        Uses Row objects + a fully-declared nested StructType so PySpark
        never attempts type inference from the data values.  This is the
        only approach that is safe when every row's 'value' field is None
        (the all-null GDP test case).

        Each dict must have:
            country_id, country_value, indicator_id, date (str), value (str|None)
        """
        from pyspark.sql import Row

        raw_rows = [
            Row(
                country=Row(id=r["country_id"], value=r["country_value"]),
                indicator=Row(id=r["indicator_id"]),
                date=r["date"],
                value=r["value"],   # may be None — schema handles it
            )
            for r in rows
        ]
        return spark.createDataFrame(raw_rows, schema=self._RAW_SCHEMA)

    # ------------------------------------------------------------------
    # Tests
    # ------------------------------------------------------------------

    def test_columns_renamed_correctly(self, job, spark):
        """Output must have exactly: country_name, country_iso3, indicator_id, year, gdp_usd."""
        raw_df = self._make_raw_df(spark, [
            {"country_id": "US", "country_value": "United States",
             "indicator_id": "NY.GDP.MKTP.CD", "date": "2022", "value": "25000000000000"}
        ])

        result = job.clean_data(raw_df)

        assert set(result.columns) == {"country_name", "country_iso3", "indicator_id", "year", "gdp_usd"}

    def test_column_types_cast_correctly(self, job, spark):
        """year must be IntegerType; gdp_usd must be DoubleType."""
        raw_df = self._make_raw_df(spark, [
            {"country_id": "US", "country_value": "United States",
             "indicator_id": "NY.GDP.MKTP.CD", "date": "2022", "value": "25000000000000.0"}
        ])

        result = job.clean_data(raw_df)
        schema = {f.name: f.dataType for f in result.schema.fields}

        assert isinstance(schema["year"],    IntegerType)
        assert isinstance(schema["gdp_usd"], DoubleType)

    def test_values_mapped_correctly(self, job, spark):
        """Spot-check that values end up in the right columns."""
        raw_df = self._make_raw_df(spark, [
            {"country_id": "BR", "country_value": "Brazil",
             "indicator_id": "NY.GDP.MKTP.CD", "date": "2021", "value": "1600000000000.0"}
        ])

        row = job.clean_data(raw_df).collect()[0]

        assert row["country_iso3"]  == "BR"
        assert row["country_name"]  == "Brazil"
        assert row["indicator_id"]  == "NY.GDP.MKTP.CD"
        assert row["year"]          == 2021
        assert row["gdp_usd"]       == pytest.approx(1.6e12)

    def test_null_gdp_rows_dropped(self, job, spark):
        """Rows with a null value (gdp_usd) must be filtered out."""
        raw_df = self._make_raw_df(spark, [
            {"country_id": "US", "country_value": "United States",
             "indicator_id": "NY.GDP.MKTP.CD", "date": "2022", "value": "25000000000000.0"},
            {"country_id": "XX", "country_value": "No Data Land",
             "indicator_id": "NY.GDP.MKTP.CD", "date": "2020", "value": None},
        ])

        result = job.clean_data(raw_df)

        assert result.count() == 1
        assert result.collect()[0]["country_iso3"] == "US"

    def test_all_null_gdp_returns_empty_dataframe(self, job, spark):
        """If every row has null value, the result should be an empty DataFrame."""
        raw_df = self._make_raw_df(spark, [
            {"country_id": "XX", "country_value": "Empty",
             "indicator_id": "NY.GDP.MKTP.CD", "date": "2020", "value": None},
            {"country_id": "YY", "country_value": "Also Empty",
             "indicator_id": "NY.GDP.MKTP.CD", "date": "2019", "value": None},
        ])

        result = job.clean_data(raw_df)

        assert result.count() == 0

    def test_multiple_valid_rows_all_retained(self, job, spark):
        """Multiple rows with valid GDP values must all survive cleaning."""
        raw_rows = [
            {"country_id": "US", "country_value": "United States",
             "indicator_id": "NY.GDP.MKTP.CD", "date": "2022", "value": "25000000000000.0"},
            {"country_id": "CN", "country_value": "China",
             "indicator_id": "NY.GDP.MKTP.CD", "date": "2022", "value": "18000000000000.0"},
            {"country_id": "DE", "country_value": "Germany",
             "indicator_id": "NY.GDP.MKTP.CD", "date": "2022", "value": "4000000000000.0"},
        ]
        raw_df = self._make_raw_df(spark, raw_rows)

        result = job.clean_data(raw_df)

        assert result.count() == 3