-- All active stations in Denmark and Greenland
SELECT "stationId", country, type, name, "operationFrom", "validFrom", 
latitude, longitude, parameters, "distanceAarhus", "distanceOdense", "distanceBallerup"
FROM stations
WHERE "operationTo" IS NULL 
AND "validTo" IS NULL
AND status='Active'
ORDER BY "stationId";
