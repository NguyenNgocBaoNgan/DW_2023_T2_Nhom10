INSERT INTO weather_day_records (province, date_record, time_record, date_forcast, temperature, feel_like, description, humidity, cloud_cover, precipitation, accumulation)
SELECT
    weather_warehouse.aggrigate.province_name,
    weather_warehouse.aggrigate.date_record,
    weather_warehouse.aggrigate.time_record,
    weather_warehouse.aggrigate.date_forcast,
    AVG(weather_warehouse.aggrigate.temperature),
    AVG(weather_warehouse.aggrigate.feel_like) as feel_like,
    (
        SELECT weather_warehouse.aggrigate.description_name
        FROM weather_warehouse.aggrigate
        GROUP BY weather_warehouse.aggrigate.description_name
        ORDER BY COUNT(weather_warehouse.aggrigate.description_name) DESC LIMIT 1
    ) as description_name,
    AVG(weather_warehouse.aggrigate.humidity) as humidity,
    AVG(weather_warehouse.aggrigate.cloud_cover) as cloud_cover,
    AVG(weather_warehouse.aggrigate.precipitation) as precipitation,
    SUM(weather_warehouse.aggrigate.accumulation) as accumulation
FROM weather_warehouse.aggrigate
GROUP BY weather_warehouse.aggrigate.province_name,weather_warehouse.aggrigate.date_record
ON DUPLICATE KEY UPDATE
                     temperature = VALUES(temperature),
                     feel_like = VALUES(feel_like),
                     description = VALUES(description),
                     humidity = VALUES(humidity),
                     cloud_cover = VALUES(cloud_cover),
                     precipitation  = VALUES(precipitation),
                     accumulation   = VALUES(accumulation);
