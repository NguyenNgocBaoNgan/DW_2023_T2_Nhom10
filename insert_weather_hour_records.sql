

INSERT INTO weather_mart.weather_hour_records (province, time_record, date_record, time_forcast, date_forcast,
                                               temperature, feel_like, description, wind_direction, wind_speed,
                                               humidity, uv_index, cloud_cover, precipitation, accumulation)
SELECT weather_warehouse.aggrigate.province_name,
       weather_warehouse.aggrigate.time_record,
       weather_warehouse.aggrigate.date_record,
       weather_warehouse.aggrigate.time_forcast,
       weather_warehouse.aggrigate.date_forcast,
       weather_warehouse.aggrigate.temperature,
       weather_warehouse.aggrigate.feel_like,
       weather_warehouse.aggrigate.description_name,
       weather_warehouse.aggrigate.wind_direction_name,
       weather_warehouse.aggrigate.wind_speed,
       weather_warehouse.aggrigate.humidity,
       weather_warehouse.aggrigate.uv_index,
       weather_warehouse.aggrigate.cloud_cover,
       weather_warehouse.aggrigate.precipitation,
       weather_warehouse.aggrigate.accumulation
FROM weather_warehouse.aggrigate
ON DUPLICATE KEY UPDATE temperature    = VALUES(temperature),
                        time_record    = VALUES(time_record),
                        date_record    = VALUES(date_record),
                        feel_like      = VALUES(feel_like),
                        wind_direction = VALUES(wind_direction),
                        wind_speed     = VALUES(wind_speed),
                        description    = VALUES(description),
                        humidity       = VALUES(humidity),
                        uv_index       = VALUES(uv_index),
                        cloud_cover    = VALUES(cloud_cover),
                        precipitation  = VALUES(precipitation),
                        accumulation   = VALUES(accumulation);