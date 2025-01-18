-- 1. Create Presentation Schema
-- CREATE SCHEMA presentation;

-- 2. Create View
CREATE OR REPLACE VIEW presentation.air_quality_data AS (
	-- The WITH keyword is part of a Common Table Expression (CTE), which is like a temporary result set you can refer to within the main query. Here's what it does in this context:
	-- ranked_data is the name of the temporary result set created by the CTE. You can think of it as a "mini-table" that only exists while the query runs.
	-- The code inside the parentheses ( ... ) defines what ranked_data contains. In this case:
	WITH ranked_data AS (
		-- 4. SELECT *, ROW_NUMBER() AS rn. After assigning the row numbers, all columns from the table and the new rn (row number) column are selected into the ranked_data CTE.
		SELECT 
			*,
			-- 3. ROW_NUMBER() OVER (...) For the remaining rows, the ROW_NUMBER() function assigns a unique number to each row within a group defined by the PARTITION BY clause. The groups are based on the combination of location_id, sensors_id, datetime, and parameter. Within each group, the rows are sorted in descending order of ingestion_datetime (latest first).
			ROW_NUMBER() OVER (
				PARTITION BY location_id, sensors_id, "datetime", "parameter"
				ORDER BY ingestion_datetime DESC
			) AS rn -- row_number
		-- 1. It selects data from the raw.air_quality_data table.
		FROM raw.air_quality_data
		-- 2. It only includes rows where parameter is one of the specified pollutants (co, no, no2, etc.).
		WHERE parameter IN ('co', 'no', 'no2', 'nox', 'o3', 'pm25')
	)
	SELECT 
		location_id,
		sensors_id,
		"location", 
		"datetime", 
		lat,
		lon,
		"parameter", 
		units,
		"value", 
		"month",
		"year", 
		ingestion_datetime
	FROM ranked_data
	WHERE rn = 1
);