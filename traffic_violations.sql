
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

-- Do certain demographics correlate with specific violation types
SELECT race,violation_type, COUNT(*) as violation_count
FROM traffic_violations
GROUP BY race,violation_type
ORDER BY violation_type,violation_count DESC;

SELECT gender,violation_type,COUNT(*) AS violation_count
FROM traffic_violations
WHERE gender IS NOT NULL
  AND violation_type IS NOT NULL
GROUP BY gender, violation_type
ORDER BY gender, violation_count DESC;

-- How does violation frequency vary by time of day, weekday, or month?
SELECT EXTRACT(HOUR FROM "timestamp") AS hour_of_day,
       COUNT(*) AS violation_count
FROM traffic_violations
WHERE "timestamp" IS NOT NULL
GROUP BY hour_of_day
ORDER BY hour_of_day;

SELECT EXTRACT(DAY FROM date_of_stop) AS day_of_month,
       COUNT(*) AS violation_count
FROM traffic_violations
WHERE date_of_stop IS NOT NULL
GROUP BY day_of_week
ORDER BY day_of_week;
 
SELECT TO_CHAR(date_of_stop, 'Day') AS weekday,
       EXTRACT(DOW FROM date_of_stop) AS weekday_num,
       COUNT(*) AS violation_count
FROM traffic_violations
WHERE date_of_stop IS NOT NULL
GROUP BY weekday, weekday_num
ORDER BY weekday_num;

SELECT TO_CHAR(date_of_stop, 'Month') AS month_of_year,
	   EXTRACT(MONTH FROM date_of_stop) AS month_num,
       COUNT(*) as violation_count
FROM traffic_violations
WHERE date_of_stop IS NOT NULL
GROUP BY month_of_year, month_num
ORDER BY month_num;

--What types of vehicles are most often involved in violations?
SELECT vehicle_category,
       COUNT(*) as violation_count
FROM traffic_violations
GROUP BY vehicle_category
ORDER BY violation_count DESC;


-- How often do violations involve accidents, injuries, or vehicle damage?
SELECT accident,
       COUNT(*) as violation_count
FROM traffic_violations
GROUP BY accident
ORDER BY violation_count;

SELECT
    COUNT(*) AS total_violations,
    COUNT(*) FILTER (WHERE accident = TRUE) AS accident_count,
    COUNT(*) FILTER (WHERE personal_injury = TRUE) AS injury_count,
    COUNT(*) FILTER (WHERE property_damage = TRUE) AS property_damage_count,
    COUNT(*) FILTER (WHERE fatal = TRUE) AS fatal_count
FROM traffic_violations;

SELECT
    ROUND(100.0 * COUNT(*) FILTER (WHERE accident = TRUE) / COUNT(*), 2) AS accident_pct,
    ROUND(100.0 * COUNT(*) FILTER (WHERE personal_injury = TRUE) / COUNT(*), 2) AS injury_pct,
    ROUND(100.0 * COUNT(*) FILTER (WHERE property_damage = TRUE) / COUNT(*), 2) AS property_damage_pct,
    ROUND(100.0 * COUNT(*) FILTER (WHERE fatal = TRUE) / COUNT(*), 2) AS fatal_pct
FROM traffic_violations;


SELECT * FROM traffic_violations LIMIT 2;

	   


