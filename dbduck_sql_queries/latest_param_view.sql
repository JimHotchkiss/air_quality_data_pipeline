CREATE OR REPLACE VIEW presentation.latest_param_values_per_location AS 
WITH ranked_data AS (
	-- The issue lies in SELECT *. When you add a ROW_NUMBER() or any window function, it must be explicitly defined in the SELECT clause alongside the other columns you want to retrieve. 
	-- SQL does not allow mixing * with window functions directly in this way.
	-- You need to add a comma, signifying a new column
	SELECT 
		location_id,
		location,
		lat,
		lon,
		parameter,
		value,
		datetime,
		-- Note: The ROW_NUMBER() Window Function needs to be treated like any other column
		ROW_NUMBER() OVER (PARTITION BY location_id, parameter ORDER BY "datetime" DESC) AS rn
	FROM 
		presentation.air_quality_data

)
-- Pivot turns identified row values, in this case, ('co', 'no', 'no2', 'nox', 'o3', 'pm25'), into columns. Also, this is why we use the ROW_NUMBER() function. We can turn a multiple rows, of the same rn value, into one row. This makes data analysis much easier 
PIVOT (
	SELECT 
		location_id,
		location,
		lat,
		lon,
		parameter,
		value,
		datetime,
	FROM ranked_data
	WHERE rn =1
)
ON "parameter" IN ('co', 'no', 'no2', 'nox', 'o3', 'pm25')
USING FIRST ("value");