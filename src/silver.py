# Databricks notebook source
catalog = "nina_john_tyler_catalog"
schema = "john_neil"
bronze_table = f"{catalog}.{schema}.weather_bronze"
silver_table = f"{catalog}.{schema}.weather_silver"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

spark.sql(
    f"""
CREATE OR REPLACE TABLE {silver_table} AS
SELECT
  country_code,
  year(date) AS year,
  month(date) AS month,
  date_format(date, 'yyyy-MM') AS year_month,
  avg(temperature_avg) AS temperature_avg,
  max(temperature_max) AS temperature_max,
  min(temperature_min) AS temperature_min,
  avg(cloud_cover_perc_avg) AS cloud_cover_perc_avg,
  max(cloud_cover_perc_max) AS cloud_cover_perc_max,
  min(cloud_cover_perc_min) AS cloud_cover_perc_min,
  count(*) AS record_count
FROM {bronze_table}
GROUP BY
  country_code,
  year(date),
  month(date),
  date_format(date, 'yyyy-MM')
"""
)

