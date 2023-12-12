TRUNCATE aggregate;
INSERT INTO aggregate(province_name, date_record, time_record, time_forcast, date_forcast, temperature, feel_like,
                      description_name, wind_direction_name,
                      wind_speed, humidity, uv_index, cloud_cover, precipitation, accumulation)
SELECT province_dim.name_vi,
       date_dim.full_date,
       records.time_record,
       records.time_forcast,
       date_dim2.full_date,
       records.temperature,
       records.feel_like,
       description_dim.name_vi,
       wind_direction_dim.name_vi,
       records.wind_speed,
       records.humidity,
       records.uv_index,
       records.cloud_cover,
       records.precipitation,
       records.accumulation
FROM records
         JOIN province_dim ON records.province_id = province_dim.id
         JOIN date_dim ON records.date_record = date_dim.date_key
         JOIN date_dim AS date_dim2 ON records.date_forcast = date_dim2.date_key
         JOIN description_dim ON records.description_id = description_dim.id
         JOIN wind_direction_dim ON records.wind_direction_id = wind_direction_dim.id
WHERE records.is_expired = 'FALSE' ORDER BY records.id DESC
LIMIT 3024;












