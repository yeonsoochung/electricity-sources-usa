--** Process power_plants_usa table and create view **--
-- Select desired columns
-- Convert units of consumption columns from MMBtu to MWh
-- Plant "AE Hunlock 4" had 2 plant codes associated with it; redundant one (shared with another plant) is removed
DROP TABLE IF EXISTS `elec_power_plants.usa_processed`;
CREATE TABLE `elec_power_plants.usa_processed` AS (
SELECT PARSE_DATE('%Y-%m', Period) AS `Month Start`, 
	`plantCode` AS `Plant Code`, `plantName` AS `Plant Name`, 
	CASE WHEN `fuelTypeDescription` = 'Municiapl Landfill Gas' THEN 'Municipal Landfill Gas'
		WHEN `fuelTypeDescription` = 'other renewables' THEN 'Other Renewables' ELSE `fuelTypeDescription` END AS `Fuel Type`, 
	CASE WHEN `fuelTypeDescription` IN ('Hydroelectric Pumped Storage', 'Hydroelectric Conventional', 'Municiapl Landfill Gas', 
			'Wood Waste Solids', 'Solar', 'Wind', 'Geothermal', 'other renewables') THEN 'Renewable'
		WHEN `fuelTypeDescription` IN ('Coal', 'Waste Coal') THEN 'Coal'
		WHEN `fuelTypeDescription` IN ('Distillate Fuel Oil', 'Residual Fuel Oil', 'Petroleum Coke', 'Waste Oil and Other Oils') THEN 'Fuel'
		WHEN `fuelTypeDescription` IN ('Other Gases', 'Other') THEN 'Other' ELSE `fuelTypeDescription` END AS `Fuel Type Category`,
	CASE WHEN `fuelTypeDescription` IN ('Hydroelectric Pumped Storage', 'Hydroelectric Conventional') THEN 'Hydro'
		WHEN `fuelTypeDescription` IN ('Municiapl Landfill Gas', 'Wood Waste Solids') THEN 'Biomass'
		WHEN `fuelTypeDescription` = 'Solar' THEN 'Solar'
		WHEN `fuelTypeDescription` = 'Wind' THEN 'Wind'
		WHEN `fuelTypeDescription` = 'Geothermal' THEN 'Geothermal'
		WHEN `fuelTypeDescription` IN ('other renewables') THEN 'Other Renewables' ELSE NULL END AS `Renewable Type`,
	`state` AS `State ID`, `stateDescription` AS `State`,
	CASE WHEN `state` IN ('CT', 'ME', 'MA', 'NH', 'RI', 'VT', 'NJ', 'NY', 'PA') THEN 'Northeast'
		WHEN `state` IN ('IL', 'IN', 'MI', 'OH', 'WI', 'IA', 'KS', 'MN', 'MO', 'NE', 'ND', 'SD') THEN 'Midwest'
		WHEN `state` IN ('FL', 'GA', 'NC', 'SC', 'VA', 'DC', 'MD', 'DE', 'WV', 'AL', 'KY', 'MS', 'TN', 'AR', 'LA', 'OK', 'TX') THEN 'South'
		WHEN `state` IN ('AZ', 'CO', 'ID', 'MT', 'NV', 'NM', 'UT', 'WY', 'AK', 'CA', 'HI', 'OR', 'WA') THEN 'West' END AS `Region`,
	0.293071 * `consumption-for-eg-btu` AS `Consumption for EG in MWh`,
	0.293071 * `total-consumption-btu` AS `Total Consumption in MWh`,
	`generation` AS `Generation in MWh`, `gross-generation` AS `Gross Generation in MWh`
FROM `elec_power_plants.usa_raw`
WHERE `fuelTypeDescription` <> 'Total' 
	AND NOT (
        `plantCode` = 56212 AND 
        `plantName` = 'AE Hunlock 4'
    ) );