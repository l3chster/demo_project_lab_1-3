from pyspark.sql import DataFrame
from pyspark.sql.functions import col, year, month, dayofmonth, hour, abs, hash

def enrich_date_dimensions(df: DataFrame, date_col: str = "date_id") -> DataFrame:
    # Extracts year, month, day and hour from date_col
    return (
        df.withColumn("year", year(col(date_col)))
        .withColumn("month", month(col(date_col)))
        .withColumn("day", dayofmonth(col(date_col)))
        .withColumn("hour", hour(col(date_col)))
    )

def generate_city_hash(df: DataFrame, city_col: str = "city") -> DataFrame:
    # Generates unique city_id based on city name
    return df.withColumn("city_id", abs(hash(col(city_col))))