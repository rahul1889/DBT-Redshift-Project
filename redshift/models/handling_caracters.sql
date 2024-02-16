-- Handling unwanted characters from columns

SELECT
  website,
  year,
  CASE WHEN pagepath = '/' THEN 'Home' 
       WHEN pagepath = '//' THEN 'home'
  ELSE pagepath END AS pagepath,
  pageviews,
  avgtimeonpage,
  entrances,
  bouncerate,
  exitrate
FROM
  {{ ref('website_traffic') }}