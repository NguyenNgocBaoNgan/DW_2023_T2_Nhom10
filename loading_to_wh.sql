SQL_SELECT_CONFIG_DATA: SELECT * FROM configuration;
SQL_UPDATE_CONFIG_STATUS: UPDATE configuration SET status = ? WHERE flag = ?;
SQL_UPDATE_DATA_EXPIRED: UPDATE records SET is_expired = ? WHERE date_forcast = ? AND time_forcast = ?;

SQL_INSERT_DATA: INSERT INTO records (province_id, time_record, date_record, time_forcast, date_forcast, temperature, feel_like, description_id, wind_direction_id, wind_speed, humidity, uv_index, cloud_cover, precipitation, accumulation) SELECT province, time_record, date_record, time_forcast, date_forcast, temperature, feel_like, description, wind_direction, wind_speed, humidity, uv_index, cloud_cover, precipitation, accumulation FROM records_staging;

SQL_CHECK_DATA_EXISTS: UPDATE records r INNER JOIN ( SELECT MIN(id) AS id FROM records GROUP BY province_id, date_record, time_record HAVING COUNT(*) > 1 ) dup ON r.id = dup.id SET r.is_expired = 'TRUE';