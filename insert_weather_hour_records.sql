

INSERT INTO weather_mart.weather_hour_records (province, time_record, date_record, time_forcast, date_forcast,
                                               temperature, feel_like, description, wind_direction, wind_speed,
                                               humidity, uv_index, cloud_cover, precipitation, accumulation)
SELECT weather_warehouse.aggregate.province_name,
       weather_warehouse.aggregate.time_record,
       weather_warehouse.aggregate.date_record,
       weather_warehouse.aggregate.time_forcast,
       weather_warehouse.aggregate.date_forcast,
       weather_warehouse.aggregate.temperature,
       weather_warehouse.aggregate.feel_like,
       weather_warehouse.aggregate.description_name,
       weather_warehouse.aggregate.wind_direction_name,
       weather_warehouse.aggregate.wind_speed,
       weather_warehouse.aggregate.humidity,
       weather_warehouse.aggregate.uv_index,
       weather_warehouse.aggregate.cloud_cover,
       weather_warehouse.aggregate.precipitation,
       weather_warehouse.aggregate.accumulation
FROM weather_warehouse.aggregate
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