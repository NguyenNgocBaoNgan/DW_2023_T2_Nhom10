SQL_SELECT_CONFIG_DATA: SELECT * FROM configuration;
SQL_UPDATE_CONFIG_STATUS: UPDATE configuration SET status = ? WHERE flag = ?;
SQL_UPDATE_DATA_EXPIRED: UPDATE records SET is_expired = ? WHERE date_forcast = ? AND time_forcast = ?;

SQL_INSERT_DATA: INSERT INTO records (province_id, time_record, date_record, time_forcast, date_forcast, temperature, feel_like, description_id, wind_direction_id, wind_speed, humidity, uv_index, cloud_cover, precipitation, accumulation) SELECT province, time_record, date_record, time_forcast, date_forcast, temperature, feel_like, description, wind_direction, wind_speed, humidity, uv_index, cloud_cover, precipitation, accumulation FROM records_staging;

SQL_CHECK_DATA_EXISTS: UPDATE records r1 INNER JOIN (  SELECT MAX(id) AS max_id, date_forcast, time_forcast, province_id   FROM records  GROUP BY date_forcast, time_forcast, province_id   HAVING COUNT(*) > 1) r2  ON r1.date_forcast = r2.date_forcast  AND r1.time_forcast = r2.time_forcast AND r1.province_id = r2.province_id SET r1.is_expired = 'TRUE' WHERE r1.id < r2.max_id;