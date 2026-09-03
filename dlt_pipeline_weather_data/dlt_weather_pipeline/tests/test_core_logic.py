import pytest
from datetime import datetime
from pyspark.sql import SparkSession
from chispa.dataframe_comparer import assert_df_equality
from transformations.core_logic import enrich_date_dimensions, generate_city_hash

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.getOrCreate()

def test_enrich_date_dimensions(spark):
    input_df = spark.createDataFrame(
        [(datetime(2026, 9, 3, 12, 38, 13),)],
        ["date_id"]
    )

    expected_df = spark.createDataFrame(
        [(datetime(2026, 9, 3, 12, 38, 13), 2026, 9, 3, 12)],
        ["date_id", "year", "month", "day", "hour"]
    ).selectExpr(
        "date_id",
        "CAST(year AS int) AS year",
        "CAST(month AS int) AS month",
        "CAST(day AS int) AS day",
        "CAST(hour AS int) AS hour"
    )

    result_df = enrich_date_dimensions(input_df)

    assert_df_equality(result_df, expected_df, ignore_nullable=True)

def test_generate_city_hash(spark):
    input_df = spark.createDataFrame([("Kraków",), ("Warszawa",)], ["city"])
    result_df = generate_city_hash(input_df)

    assert "city_id" in result_df.columns
    assert result_df.filter(result_df["city_id"].isNull()).count() == 0