--1_SETTING UP THE STRUCTURES FOR LOADING DATA
-- Create the database
CREATE DATABASE olympic_analysis;

-- Create the schema within the database
CREATE SCHEMA olympic_data;

-- Create the stage;this will be the temporary storage location where to upload the files before loading them in tables
CREATE OR REPLACE STAGE olympic_stage;

--2_CREATING TABLES 
-- Create the nocs table
CREATE OR REPLACE TABLE olympic_data.nocs_temp (
    code VARCHAR,
    country VARCHAR,
    country_long VARCHAR,
    tag VARCHAR,
    note VARCHAR
);


-- Create the events table
CREATE OR REPLACE TABLE olympic_data.events_temp (
    event VARCHAR,
    tag VARCHAR,
    sport VARCHAR,
    sport_code VARCHAR,
    sport_url VARCHAR
);

-- Create the athletes table
CREATE OR REPLACE TABLE olympic_data.athletes_temp (
    code VARCHAR,
    name VARCHAR,
    name_short VARCHAR,
    name_tv VARCHAR,
    gender VARCHAR,
    function VARCHAR,
    country_code VARCHAR,
    country VARCHAR,
    country_long VARCHAR,
    nationality VARCHAR,
    nationality_full VARCHAR,
    nationality_code VARCHAR,
    height INT,
    weight FLOAT,
    disciplines VARCHAR,
    events VARCHAR,
    birth_date DATE,
    birth_place VARCHAR,
    birth_country VARCHAR,
    residence_place VARCHAR,
    residence_country VARCHAR,
    nickname VARCHAR,
    hobbies VARCHAR,
    occupation VARCHAR,
    education VARCHAR,
    family VARCHAR,
    lang VARCHAR,
    coach VARCHAR,
    reason VARCHAR,
    hero VARCHAR,
    influence VARCHAR,
    philosophy VARCHAR,
    sporting_relatives VARCHAR,
    ritual VARCHAR,
    other_sports VARCHAR
);

-- Create the medals table
CREATE OR REPLACE TABLE olympic_data.medals_temp (
    medal_type VARCHAR,
    medal_code FLOAT,
    medal_date DATE,
    name VARCHAR,
    gender VARCHAR,
    discipline VARCHAR,
    event VARCHAR,
    event_type VARCHAR,
    url_event VARCHAR,
    code VARCHAR,
    country_code VARCHAR,
    country VARCHAR,
    country_long VARCHAR
);



-- Create the venues table
CREATE OR REPLACE TABLE olympic_data.venues_temp (
    venue VARCHAR,
    sports VARCHAR,
    date_start DATE,
    date_end DATE,
    tag VARCHAR,
    url VARCHAR
);


--3_LOADING DATA FROM STAGE TO THE TABLES
-- Load data into athletes_temp
COPY INTO olympic_data.athletes_temp
FROM @olympic_stage/athletes.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER = 1);

SELECT * FROM olympic_data.athletes_temp LIMIT 10;

-- Load data into medals_temp
COPY INTO olympic_data.medals_temp
FROM @olympic_stage/medals.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER = 1);

SELECT * FROM olympic_data.medals_temp LIMIT 10;

-- Load data into events_temp
COPY INTO olympic_data.events_temp
FROM @olympic_stage/events.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER = 1);

SELECT * FROM olympic_data.events_temp LIMIT 10;

-- Load data into nocs_temp
COPY INTO olympic_data.nocs_temp
FROM @olympic_stage/nocs.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER = 1);

SELECT * FROM olympic_data.nocs_temp LIMIT 10;

-- Load data into venues_temp
COPY INTO olympic_data.venues_temp
FROM @olympic_stage/venues.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER = 1);

SELECT * FROM olympic_data.venues_temp LIMIT 10;

--4_FINAL TABLES CLEANED
-- Create the nocs final table
CREATE TABLE olympic_data.nocs (
    code VARCHAR PRIMARY KEY,
    country VARCHAR,
    country_long VARCHAR,
    tag VARCHAR,
    note VARCHAR
) AS
SELECT DISTINCT * FROM olympic_data.nocs_temp;

-- Create the events final table
CREATE TABLE olympic_data.events (
    event VARCHAR PRIMARY KEY,
    tag VARCHAR,
    sport VARCHAR,
    sport_code VARCHAR,
    sport_url VARCHAR
) AS
SELECT DISTINCT * FROM olympic_data.events_temp;

-- Create the athletes final table
CREATE TABLE olympic_data.athletes (
    code VARCHAR PRIMARY KEY,
    name VARCHAR,
    name_short VARCHAR,
    name_tv VARCHAR,
    gender VARCHAR,
    function VARCHAR,
    country_code VARCHAR,
    country VARCHAR,
    country_long VARCHAR,
    nationality VARCHAR,
    nationality_full VARCHAR,
    nationality_code VARCHAR,
    height INT,
    weight FLOAT,
    disciplines VARCHAR,
    events VARCHAR,
    birth_date DATE,
    birth_place VARCHAR,
    birth_country VARCHAR,
    residence_place VARCHAR,
    residence_country VARCHAR,
    nickname VARCHAR,
    hobbies VARCHAR,
    occupation VARCHAR,
    education VARCHAR,
    family VARCHAR,
    lang VARCHAR,
    coach VARCHAR,
    reason VARCHAR,
    hero VARCHAR,
    influence VARCHAR,
    philosophy VARCHAR,
    sporting_relatives VARCHAR,
    ritual VARCHAR,
    other_sports VARCHAR,
    FOREIGN KEY (country_code) REFERENCES olympic_data.nocs(code)
) AS
SELECT DISTINCT * FROM olympic_data.athletes_temp;

-- Create the medals final table
CREATE TABLE olympic_data.medals (
    medal_type VARCHAR,
    medal_code FLOAT,
    medal_date DATE,
    name VARCHAR,
    gender VARCHAR,
    discipline VARCHAR,
    event VARCHAR,
    event_type VARCHAR,
    url_event VARCHAR,
    code VARCHAR,
    country_code VARCHAR,
    country VARCHAR,
    country_long VARCHAR,
    PRIMARY KEY (code, event),
    FOREIGN KEY (code) REFERENCES olympic_data.athletes(code),
    FOREIGN KEY (country_code) REFERENCES olympic_data.nocs(code),
    FOREIGN KEY (event) REFERENCES olympic_data.events(event)
) AS
SELECT DISTINCT * FROM olympic_data.medals_temp;

-- Create the venues final table
CREATE TABLE olympic_data.venues (
    venue VARCHAR,
    sports VARCHAR,
    date_start DATE,
    date_end DATE,
    tag VARCHAR,
    url VARCHAR,
    PRIMARY KEY (venue, sports)
) AS
SELECT DISTINCT * FROM olympic_data.venues_temp;

--5_ANALYSIS
--Medal Distribution Analysis--
--Medal Trends by Discipline
SELECT
    e.sport AS discipline,
    COUNT(m.medal_type) AS total_medals
FROM olympic_data.medals m
INNER JOIN olympic_data.events e ON m.event = e.event
GROUP BY e.sport
ORDER BY e.sport;

--Medal Count by Athlete and Gender
SELECT
    a.gender,
    e.sport,
    COUNT(m.medal_type) AS total_medals
FROM olympic_data.athletes a
LEFT JOIN olympic_data.medals m ON a.code = m.code
LEFT JOIN olympic_data.events e ON m.event = e.event
GROUP BY a.gender, e.sport
ORDER BY total_medals DESC;


--Medal Distribution by Continent
SELECT
    CASE
        WHEN n.country IN ('Afghanistan', 'India', 'China', 'Japan', 'South Korea', 'Saudi Arabia', 'United Arab Emirates') THEN 'Asia'
        WHEN n.country IN ('United States', 'Canada', 'Mexico') THEN 'North America'
        WHEN n.country IN ('Brazil', 'Argentina', 'Chile') THEN 'South America'
        WHEN n.country IN ('France', 'Germany', 'Italy', 'United Kingdom', 'Spain') THEN 'Europe'
        WHEN n.country IN ('Australia', 'New Zealand') THEN 'Oceania'
        WHEN n.country IN ('South Africa', 'Nigeria', 'Kenya', 'Egypt') THEN 'Africa'
        ELSE 'Other'
    END AS continent,
    COUNT(m.medal_type) AS total_medals
FROM olympic_data.medals m
INNER JOIN olympic_data.nocs n ON m.country_code = n.code
GROUP BY continent
ORDER BY total_medals DESC;

--Running Total of Medals for Each Country
SELECT
    n.country_long AS country_name,
    COUNT(m.medal_type) AS total_medals
FROM olympic_data.medals m
INNER JOIN olympic_data.nocs n ON m.country_code = n.code
GROUP BY n.country_long
ORDER BY total_medals DESC, country_name;

--Top Performing Countries by Sport
SELECT
    e.sport,
    n.country_long AS country_name,
    COUNT(m.medal_type) AS total_medals
FROM olympic_data.medals m
INNER JOIN olympic_data.events e ON m.event = e.event
INNER JOIN olympic_data.nocs n ON m.country_code = n.code
GROUP BY e.sport, n.country_long
ORDER BY e.sport, total_medals DESC;


--Country Performance Analysis--
--Performance by Country and Sport
SELECT
    n.country_long AS country_name,
    e.sport,
    COUNT(m.medal_type) AS total_medals
FROM olympic_data.medals m
INNER JOIN olympic_data.nocs n ON m.country_code = n.code
INNER JOIN olympic_data.events e ON m.event = e.event
GROUP BY n.country_long, e.sport
ORDER BY total_medals DESC;

--Medal Efficiency by Country
SELECT
    n.country_long AS country_name,
    COUNT(DISTINCT a.code) AS total_athletes,
    COUNT(m.medal_type) AS total_medals,
    CASE 
        WHEN COUNT(DISTINCT a.code) = 0 THEN 0 
        ELSE COUNT(m.medal_type) / COUNT(DISTINCT a.code) 
    END AS medal_efficiency
FROM olympic_data.athletes a
LEFT JOIN olympic_data.medals m ON a.code = m.code
INNER JOIN olympic_data.nocs n ON a.country_code = n.code
GROUP BY n.country_long
ORDER BY medal_efficiency DESC;

--Medal Efficiency Ranking Using CTE
WITH MedalEfficiency AS (
    SELECT
        n.country_long AS country_name,
        COUNT(DISTINCT a.code) AS total_athletes,
        COUNT(m.medal_type) AS total_medals,
        COUNT(m.medal_type) / COUNT(DISTINCT a.code) AS efficiency
    FROM olympic_data.athletes a
    LEFT JOIN olympic_data.medals m ON a.code = m.code
    INNER JOIN olympic_data.nocs n ON a.country_code = n.code
    GROUP BY n.country_long
)
SELECT
    country_name,
    total_athletes,
    total_medals,
    efficiency,
    RANK() OVER (ORDER BY efficiency DESC) AS efficiency_rank
FROM MedalEfficiency;


--Event Performance Analysis--
--Most Successful Events
SELECT
    e.event AS event_name,
    n.country_long AS country_name,
    COUNT(m.medal_type) AS total_medals
FROM olympic_data.medals m
INNER JOIN olympic_data.events e ON m.event = e.event
INNER JOIN olympic_data.nocs n ON m.country_code = n.code
GROUP BY e.event, n.country_long
ORDER BY total_medals DESC;

--Most Successful Events & Sports
SELECT
    e.event AS event_name,
    e.sport AS sport_type,
    n.country_long AS country_name,
    COUNT(m.medal_type) AS total_medals
FROM olympic_data.medals m
INNER JOIN olympic_data.events e ON m.event = e.event
INNER JOIN olympic_data.nocs n ON m.country_code = n.code
GROUP BY e.event, e.sport, n.country_long
ORDER BY total_medals DESC, e.sport ASC, n.country_long ASC;

--Most Successful Events & Sports (France)
SELECT
    e.event AS event_name,
    e.sport AS sport_type,
    n.country_long AS country_name,
    COUNT(m.medal_type) AS total_medals
FROM olympic_data.medals m
INNER JOIN olympic_data.events e ON m.event = e.event
INNER JOIN olympic_data.nocs n ON m.country_code = n.code
WHERE n.country_long ='France'
GROUP BY e.event, e.sport, n.country_long
ORDER BY total_medals DESC, e.sport ASC;

--Total Medals of a Country in an Event (France in Men's Events)
SELECT
    e.event AS event_name,
    n.country_long AS country_name,
    COUNT(m.medal_type) AS total_medals
FROM olympic_data.medals m
INNER JOIN olympic_data.nocs n ON m.country_code = n.code
INNER JOIN olympic_data.events e ON m.event = e.event
WHERE n.country_long = 'France' AND e.event LIKE 'Men'
GROUP BY e.event, n.country_long;


--Athlete Performance Analysis--
--Multi-Medal Athletes
SELECT
    a.name,
    COUNT(m.medal_type) AS total_medals
FROM olympic_data.athletes a
INNER JOIN olympic_data.medals m ON a.code = m.code
GROUP BY a.name
HAVING COUNT(m.medal_type) > 1
ORDER BY total_medals DESC, name ASC;

--Age of Medal Winners
SELECT
    a.birth_date,
    DATEDIFF(year, a.birth_date, m.medal_date) AS age_at_olympics,
    COUNT(m.medal_type) AS total_medals
FROM olympic_data.athletes a
INNER JOIN olympic_data.medals m ON a.code = m.code
GROUP BY a.birth_date, EXTRACT(YEAR FROM m.medal_date), age_at_olympics
ORDER BY total_medals DESC, age_at_olympics DESC;


--Business-Oriented Analysis--
--Medal Revenue Estimation for Sponsors
SELECT
    n.country_long AS country_name,
    SUM(
        CASE 
            WHEN m.medal_type LIKE '%Gold%' THEN 1000000
            WHEN m.medal_type LIKE '%Silver%' THEN 500000
            WHEN m.medal_type LIKE '%Bronze%' THEN 250000
            ELSE 0
        END
    ) AS estimated_revenue_euro
FROM olympic_data.medals m
INNER JOIN olympic_data.nocs n ON m.country_code = n.code
GROUP BY n.country_long
ORDER BY estimated_revenue_euro DESC;

--Medal Revenue Estimation by Sport and Athlete
SELECT 
    e.sport,
    a.name AS athlete_name,
    SUM(
        CASE 
            WHEN m.medal_type LIKE '%Gold%' THEN 1.5 * 1000000
            WHEN m.medal_type LIKE '%Silver%' THEN 1.2 * 500000
            WHEN m.medal_type LIKE '%Bronze%' THEN 1.1 * 250000
            ELSE 0
        END
    ) AS sponsor_value
FROM olympic_data.athletes a
INNER JOIN olympic_data.medals m ON a.code = m.code
INNER JOIN olympic_data.events e ON m.event = e.event
GROUP BY e.sport, a.name
ORDER BY sponsor_value DESC;


--Gender Disparity in Medal Distribution
SELECT
    e.sport,
    a.gender,
    COUNT(m.medal_type) AS total_medals
FROM olympic_data.athletes a
LEFT JOIN olympic_data.medals m ON a.code = m.code
INNER JOIN olympic_data.events e ON m.event = e.event
GROUP BY e.sport, a.gender
ORDER BY e.sport, total_medals DESC, a.gender;
