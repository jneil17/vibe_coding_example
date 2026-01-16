# Databricks notebook source
catalog = "nina_john_tyler_catalog"
schema = "john_neil"
silver_table = f"{catalog}.{schema}.weather_silver"
gold_table = f"{catalog}.{schema}.weather_gold"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

spark.sql(
    f"""
CREATE OR REPLACE TABLE {gold_table} AS
SELECT
  country_code,
  year,
  avg(temperature_avg) AS temperature_avg_yearly,
  max(temperature_max) AS temperature_max_yearly,
  min(temperature_min) AS temperature_min_yearly,
  count(*) AS record_count
FROM {silver_table}
GROUP BY
  country_code,
  year
"""
)

