-- Update geographic errors in power_plants table
UPDATE `elec_power_plants.power_plants` p
SET 
    City = u.City,
    State = u.State,
    `Zip Code` = u.`Zip Code`,
    Longitude = u.Longitude,
    Latitude = u.Latitude
FROM (
    SELECT 
        `Plant Code`,
        CASE WHEN `Plant Code` = 60696 THEN 'Sunland Park' ELSE City END AS City,
        CASE WHEN `Plant Code` = 67729 THEN 'Wisconsin'
             WHEN `Plant Code` = 60696 THEN 'New Mexico'
             ELSE State END AS State,
        CASE WHEN `Plant Code` = 60696 THEN 88063 ELSE `Zip Code` END AS `Zip Code`,
        CASE WHEN `Plant Code` = 67621 THEN -79.281720
             WHEN `Plant Code` = 67469 THEN -71.1697
             ELSE Longitude END AS Longitude,
        CASE WHEN `Plant Code` = 67621 THEN 42.50275
             WHEN `Plant Code` = 67469 THEN 41.8852
             ELSE Latitude END AS Latitude
    FROM `elec_power_plants.power_plants`
) AS u
WHERE p.`Plant Code` = u.`Plant Code`;

