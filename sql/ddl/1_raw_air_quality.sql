-- Note: The name convention of these SQL files will help us to identify the order in which we want these sql queries to run

-- Create our tables 
    -- Note: Per the video, Trend confused the name of the table. It is actually air_quality_data. He confused as air_quality
CREATE TABLE IF NOT EXISTS raw.air_quality(
    location_id BIGINT,
    sensors_id BIGINT,
    "location" VARCHAR, 
    "datetime" TIMESTAMP,
    lat DOUBLE,
    lon DOUBLE,
    "parameter" VARCHAR,
    units VARCHAR,
    "value" DOUBLE,
    "month" VARCHAR,
    "year" BIGINT,
    ingestion_datetime TIMESTAMP
);