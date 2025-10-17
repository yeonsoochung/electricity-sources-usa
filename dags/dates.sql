--** Create dates table **--
DROP TABLE IF EXISTS `elec_power_plants.dates`;
CREATE TABLE `elec_power_plants.dates` AS (
WITH date_range AS (
  SELECT 
    MIN(PARSE_DATE('%Y-%m', Period)) AS start_date,
    MAX(PARSE_DATE('%Y-%m', Period)) AS end_date
  FROM `elec_power_plants.usa_raw`
),
dates_cte AS (
  SELECT DATE_ADD(start_date, INTERVAL num DAY) AS Date
  FROM date_range, 
  UNNEST(GENERATE_ARRAY(0, DATE_DIFF(end_date, start_date, DAY))) AS num
)
SELECT 
    Date AS `Date`,
    FORMAT_DATE('%B', Date) AS `Month Name`,
    EXTRACT(MONTH FROM Date) AS `Month`,
    EXTRACT(YEAR FROM Date) AS `Year`,
    CASE 
        WHEN EXTRACT(MONTH FROM Date) IN (1, 2, 3) THEN 'Q1'
        WHEN EXTRACT(MONTH FROM Date) IN (4, 5, 6) THEN 'Q2'
        WHEN EXTRACT(MONTH FROM Date) IN (7, 8, 9) THEN 'Q3'
        ELSE 'Q4' 
    END AS `Quarter`,
    DATE_SUB(DATE_TRUNC(Date, WEEK), INTERVAL 1 DAY) AS `Week Start`, -- Set Sunday as start of week
    DATE_TRUNC(Date, MONTH) AS `Month Start`,
    DATE_TRUNC(Date, QUARTER) AS `Quarter Start`,
    DATE_TRUNC(Date, YEAR) AS `Year Start`
FROM dates_cte );