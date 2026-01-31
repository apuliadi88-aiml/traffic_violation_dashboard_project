DROP TABLE traffic_violations;

CREATE TABLE traffic_violations(
    seqid TEXT PRIMARY KEY,
    date_of_stop DATE,
    time_of_stop VARCHAR(8),          -- HH:MM:SS
    agency TEXT,
    subagency TEXT,
    location TEXT,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    accident BOOLEAN,
    belts BOOLEAN,
    personal_injury BOOLEAN,
    property_damage BOOLEAN,
    fatal BOOLEAN,
    commercial_license BOOLEAN,
    hazmat BOOLEAN,
    commercial_vehicle BOOLEAN,
    alcohol BOOLEAN,
    work_zone BOOLEAN,
    search_conducted BOOLEAN,
    search_disposition TEXT,
    search_outcome TEXT,
    search_reason TEXT,
    search_reason_for_stop TEXT,
    search_type TEXT,
    search_arrest_reason TEXT,
    state CHAR(2),
    vehicletype TEXT,
    year INTEGER,
    make TEXT,
    model TEXT,
    color TEXT,
    violation_type TEXT,
    article TEXT,
    contributed_to_accident BOOLEAN,
    race TEXT,
    gender TEXT,
    driver_city TEXT,
    driver_state CHAR(2),
    dl_state CHAR(2),
    arrest_type TEXT,
    geolocation TEXT,
    description TEXT,
    charge TEXT,
    "timestamp" TIMESTAMP,
    vehicle_code TEXT,
    vehicle_category TEXT
);

SELECT COUNT(*) FROM traffic_violations;

SELECT * FROM traffic_violations LIMIT 2;


-- Most Common Type Of Violation
Select DISTINCT(violation_type), COUNT(*) as violation_count
FROM traffic_violations
GROUP BY violation_type
ORDER BY violation_count DESC
LIMIT 5;

-- Which areas or coordinates have the highest traffic incidents
SELECT DISTINCT(location),  count(*) as highest_violation_count
FROM traffic_violations
GROUP BY location
ORDER BY highest_violation_count DESC
LIMIT 5;

SELECT latitude, longitude, COUNT(*) AS incident_count
FROM traffic_violations
WHERE latitude IS NOT NULL AND longitude IS NOT NULL
GROUP BY latitude, longitude
ORDER BY incident_count DESC
LIMIT 5;
