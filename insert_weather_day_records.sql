INSERT INTO weather_day_records (province, date_record, time_record, date_forcast, temperature, feel_like, description, humidity, cloud_cover, precipitation, accumulation)
SELECT
    weather_warehouse.aggregate.province_name,
    weather_warehouse.aggregate.date_record,
    weather_warehouse.aggregate.time_record,
    weather_warehouse.aggregate.date_forcast,
    AVG(weather_warehouse.aggregate.temperature),
    AVG(weather_warehouse.aggregate.feel_like) as feel_like,
    (
        SELECT weather_warehouse.aggregate.description_name
        FROM weather_warehouse.aggregate
        GROUP BY weather_warehouse.aggregate.description_name
        ORDER BY COUNT(weather_warehouse.aggregate.description_name) DESC LIMIT 1
    ) as description_name,
    AVG(weather_warehouse.aggregate.humidity) as humidity,
    AVG(weather_warehouse.aggregate.cloud_cover) as cloud_cover,
    AVG(weather_warehouse.aggregate.precipitation) as precipitation,
    SUM(weather_warehouse.aggregate.accumulation) as accumulation
FROM weather_warehouse.aggregate
GROUP BY weather_warehouse.aggregate.province_name,weather_warehouse.aggregate.date_forcast
ON DUPLICATE KEY UPDATE
                     temperature = VALUES(temperature),
                     feel_like = VALUES(feel_like),
                     description = VALUES(description),
                     humidity = VALUES(humidity),
                     cloud_cover = VALUES(cloud_cover),
                     precipitation  = VALUES(precipitation),
                     accumulation   = VALUES(accumulation);
