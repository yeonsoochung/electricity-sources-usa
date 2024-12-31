-- Database: eia

-- DROP DATABASE IF EXISTS eia;

CREATE DATABASE eia
    WITH
    OWNER = postgres
    ENCODING = 'UTF8'
    LC_COLLATE = 'English_United States.1252'
    LC_CTYPE = 'English_United States.1252'
    LOCALE_PROVIDER = 'libc'
    TABLESPACE = pg_default
    CONNECTION LIMIT = -1
    IS_TEMPLATE = False;


--** Convert power plant locations table to view **--
DROP VIEW IF EXISTS power_plants_loc_view;
CREATE VIEW power_plants_loc_view AS (
SELECT * FROM power_plants_loc );
-- SELECT * FROM power_plants_loc_view LIMIT 100;


--** Combine regional tables into one for USA **--
DROP TABLE IF EXISTS power_plants_midwest;
CREATE TABLE power_plants_midwest AS (
SELECT * FROM power_plants_midwest_1
UNION
SELECT * FROM power_plants_midwest_2
UNION
SELECT * FROM power_plants_midwest_3
UNION
SELECT * FROM power_plants_midwest_4
ORDER BY "Period", "State" );
-- SELECT * FROM power_plants_midwest LIMIT 100;

DROP TABLE IF EXISTS power_plants_northeast;
CREATE TABLE power_plants_northeast AS (
SELECT * FROM power_plants_northeast_1
UNION
SELECT * FROM power_plants_northeast_2
UNION
SELECT * FROM power_plants_northeast_3
ORDER BY "Period", "State" );
SELECT * FROM power_plants_northeast LIMIT 100;

DROP TABLE IF EXISTS power_plants_south;
CREATE TABLE power_plants_south AS (
SELECT * FROM power_plants_south_1
UNION
SELECT * FROM power_plants_south_2
UNION
SELECT * FROM power_plants_south_3
UNION
SELECT * FROM power_plants_south_4
UNION
SELECT * FROM power_plants_south_5
UNION
SELECT * FROM power_plants_south_6
ORDER BY "Period", "State" );
-- SELECT * FROM power_plants_south LIMIT 100;

DROP TABLE IF EXISTS power_plants_west;
CREATE TABLE power_plants_west AS (
SELECT * FROM power_plants_west_1
UNION
SELECT * FROM power_plants_west_2
UNION
SELECT * FROM power_plants_west_3
UNION
SELECT * FROM power_plants_west_4
UNION
SELECT * FROM power_plants_west_5
UNION
SELECT * FROM power_plants_west_6
ORDER BY "Period", "State" );
-- SELECT * FROM power_plants_west LIMIT 100;

DROP TABLE IF EXISTS power_plants_usa;
CREATE TABLE power_plants_usa AS (
SELECT * FROM power_plants_midwest
UNION
SELECT * FROM power_plants_northeast
UNION
SELECT * FROM power_plants_south
UNION
SELECT * FROM power_plants_west
ORDER BY "Period", "State" );
-- SELECT * FROM power_plants_usa LIMIT 100;


--** Create dates view **--
DROP VIEW IF EXISTS dates_view;
CREATE VIEW dates_view AS (
WITH dates_cte AS (
SELECT GENERATE_SERIES(
	(SELECT MIN(TO_DATE("Period", 'YYYY-MM')) FROM power_plants_midwest),
	(SELECT MAX(TO_DATE("Period", 'YYYY-MM')) FROM power_plants_midwest), 
	'1 day'::INTERVAL) AS "Date"
)
SELECT "Date"::DATE AS "Date",
	TO_CHAR("Date", 'Month') AS "Month Name",
	EXTRACT (MONTH FROM "Date")::INTEGER AS "Month",
	EXTRACT (YEAR FROM "Date")::INTEGER AS "Year",
	CASE WHEN EXTRACT (MONTH FROM "Date") IN (1, 2, 3) THEN 'Q1'
		  WHEN EXTRACT (MONTH FROM "Date") IN (4, 5, 6) THEN 'Q2'
		  WHEN EXTRACT (MONTH FROM "Date") IN (7, 8, 9) THEN 'Q3'
		  ELSE 'Q4' 
		  END	AS "Quarter",
	(DATE_TRUNC('week', "Date") - INTERVAL '1 day')::DATE AS "Week Start", -- Set Sunday as start of week
	DATE_TRUNC('month', "Date")::DATE AS "Month Start",
	DATE_TRUNC('quarter', "Date")::DATE AS "Quarter Start",
	DATE_TRUNC('year', "Date")::DATE AS "Year Start"
FROM dates_cte
);


--** Process power_plants_usa table and create view **--
-- Select desired columns
-- Convert units of consumption columns from MMBtu to MWh
-- Plant "AE Hunlock 4" had 2 plant codes associated with it; redundant one (shared with another plant) is removed
DROP VIEW IF EXISTS power_plants_usa_view;
CREATE VIEW power_plants_usa_view AS (
SELECT TO_DATE("Period", 'YYYY-MM') AS "Month Start", 
	"Plant Code", "Plant Name", 
	CASE WHEN "Fuel Type" = 'Municiapl Landfill Gas' THEN 'Municipal Landfill Gas'
		WHEN "Fuel Type" = 'other renewables' THEN 'Other Renewables' ELSE "Fuel Type" END AS "Fuel Type", 
	CASE WHEN "Fuel Type" IN ('Hydroelectric Pumped Storage', 'Hydroelectric Conventional', 'Municiapl Landfill Gas', 
			'Wood Waste Solids', 'Solar', 'Wind', 'Geothermal', 'other renewables') THEN 'Renewable'
		WHEN "Fuel Type" IN ('Coal', 'Waste Coal') THEN 'Coal'
		WHEN "Fuel Type" IN ('Distillate Fuel Oil', 'Residual Fuel Oil', 'Petroleum Coke', 'Waste Oil and Other Oils') THEN 'Fuel'
		WHEN "Fuel Type" IN ('Other Gases', 'Other') THEN 'Other' ELSE "Fuel Type" END AS "Fuel Type Category",
	CASE WHEN "Fuel Type" IN ('Hydroelectric Pumped Storage', 'Hydroelectric Conventional') THEN 'Hydro'
		WHEN "Fuel Type" IN ('Municiapl Landfill Gas', 'Wood Waste Solids') THEN 'Biomass'
		WHEN "Fuel Type" = 'Solar' THEN 'Solar'
		WHEN "Fuel Type" = 'Wind' THEN 'Wind'
		WHEN "Fuel Type" = 'Geothermal' THEN 'Geothermal'
		WHEN "Fuel Type" IN ('other renewables') THEN 'Other Renewables' ELSE NULL END AS "Renewable Type",
	"State ID", "State",
	CASE WHEN "State ID" IN ('CT', 'ME', 'MA', 'NH', 'RI', 'VT', 'NJ', 'NY', 'PA') THEN 'Northeast'
		WHEN "State ID" IN ('IL', 'IN', 'MI', 'OH', 'WI', 'IA', 'KS', 'MN', 'MO', 'NE', 'ND', 'SD') THEN 'Midwest'
		WHEN "State ID" IN ('FL', 'GA', 'NC', 'SC', 'VA', 'DC', 'MD', 'DE', 'WV', 'AL', 'KY', 'MS', 'TN', 'AR', 'LA', 'OK', 'TX') THEN 'South'
		WHEN "State ID" IN ('AZ', 'CO', 'ID', 'MT', 'NV', 'NM', 'UT', 'WY', 'AK', 'CA', 'HI', 'OR', 'WA') THEN 'West' END AS "Region",
	0.293071 * "Consumption for EG (MMBtu)" AS "Consumption for EG (MWh)",
	0.293071 * "Total Consumption (MMBtu)" AS "Total Consumption (MWh)",
	"Generation (MWh)", "Gross Generation (MWh)"
FROM power_plants_usa
WHERE "Fuel Type" <> 'Total' 
	AND NOT (
        "Plant Code" = 56212 AND 
        "Plant Name" = 'AE Hunlock 4'
    ) );



