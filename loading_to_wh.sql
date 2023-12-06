SQL_SELECT_CONFIG_DATA: SELECT * FROM configuration;
SQL_UPDATE_CONFIG_STATUS: UPDATE configuration SET status = ? WHERE flag = ?;
SQL_UPDATE_DATA_EXPIRED: UPDATE records SET is_expired = ? WHERE date_forcast = ? AND time_forcast = ?;

SQL_INSERT_DATA: INSERT INTO records (province_id, time_record, date_record, time_forcast, date_forcast, temperature, feel_like, description_id, wind_direction_id, wind_speed, humidity, uv_index, cloud_cover, precipitation, accumulation) SELECT province, time_record, date_record, time_forcast, date_forcast, temperature, feel_like, description, wind_direction, wind_speed, humidity, uv_index, cloud_cover, precipitation, accumulation FROM records_staging;

SQL_CHECK_DATA_EXISTS: UPDATE records AS r1 SET is_expired = 'True'WHERE (date_forcast, time_forcast) = (SELECT date_forcast, time_forcast FROM records AS r2 WHERE r1.date_forcast = r2.date_forcast AND r1.time_forcast = r2.time_forcast ORDER BY date_record DESC, time_record DESC LIMIT 1) AND (date_record, time_record) < (SELECT MAX(date_record), MAX(time_record) FROM records WHERE date_forcast = r1.date_forcast AND time_forcast = r1.time_forcast);