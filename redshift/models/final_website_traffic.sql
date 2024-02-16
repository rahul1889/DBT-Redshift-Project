-- Renaming columns as BI-friendly

SELECT
  website,
  year,
  pagepath AS page_path,
  pageviews AS total_pageviews,
  avgtimeonpage AS average_time_on_page,
  entrances,
  bouncerate AS bounce_rate,
  exitrate AS exit_rate
FROM
  {{ ref('handling_caracters') }}
