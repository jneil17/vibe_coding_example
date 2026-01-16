# Databricks notebook source
catalog = "nina_john_tyler_catalog"
schema = "john_neil"
source_table = "samples.accuweather.forecast_daily_calendar_imperial"
target_table = f"{catalog}.{schema}.weather_bronze"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

spark.sql(
    f"""
CREATE TABLE IF NOT EXISTS {target_table}
USING DELTA
AS SELECT * FROM {source_table} WHERE 1 = 0
"""
)

spark.sql(
    f"""
MERGE INTO {target_table} AS target
USING {source_table} AS source
ON target.date = source.date
  AND target.city_name <=> source.city_name
  AND target.country_code <=> source.country_code
  AND target.latitude <=> source.latitude
  AND target.longitude <=> source.longitude
WHEN NOT MATCHED THEN INSERT *
"""
)

