INSERT INTO weather_hour_records (province, time_record, date_record, time_forcast, date_forcast, temperature, feel_like, description, wind_direction, wind_speed, humidity, uv_index, cloud_cover, precipitation, accumulation, is_available)
SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
FROM aggregate 
WHERE NOT EXISTS (SELECT 1 FROM weather_hour_records WHERE time_forcast = ? AND date_forcast = ?);