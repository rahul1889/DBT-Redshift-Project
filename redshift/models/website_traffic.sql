-- models/my_model.sql

WITH cleaned_data AS (
  SELECT
    website,
    CAST(year AS INT) AS year,
    CAST(pagepath AS VARCHAR) AS pagepath,
    COALESCE(CAST(pageviews AS NUMERIC), 0) AS pageviews,
    COALESCE(CAST(avgtimeonpage AS DOUBLE PRECISION), 0) AS avgtimeonpage,
    CAST(entrances AS DOUBLE PRECISION) AS entrances,
    CAST(bouncerate AS DOUBLE PRECISION) AS bouncerate,
    CAST(exitrate AS DOUBLE PRECISION) AS exitrate
  FROM website_traffic
)
SELECT *
FROM cleaned_data
