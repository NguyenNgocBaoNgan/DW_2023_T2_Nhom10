-- INSERT INTO weather_day_records(province, date_record, time_record, date_forcast, temperature, feel_like, description_name, humidity, cloud_cover, precipitation, accumulation)
-- SELECT
--     province_name as province,
--     date_record as date_record,
--     time_record as time_record,
--     date_forcast as date_forcast,
--     AVG(temperature) as temperature,
--     AVG(feel_like) as feel_like,
--     (SELECT description_name FROM aggregate GROUP BY description_name ORDER BY COUNT(description_name) DESC LIMIT 1) as description_name,
--     AVG(humidity) as humidity,
--     AVG(cloud_cover) as cloud_cover,
--     AVG(precipitation) as precipitation,
--     SUM(accumulation) as accumulation
-- FROM aggregate;

INSERT INTO weather_day_records (province, date_record, time_record, date_forcast, temperature, feel_like, description_name, humidity, cloud_cover, precipitation, accumulation)  
SELECT
    ? as province,  
    ? as date_record,  
    ? as time_record,  
    ? as date_forcast,  
    AVG(?) as temperature,  
    AVG(?) as feel_like,  
    (SELECT ? FROM aggregate GROUP BY ? ORDER BY COUNT(?) DESC LIMIT 1) as description_name,  
    AVG(?) as humidity,  
    AVG(?) as cloud_cover,  
    AVG(?) as precipitation,  
    SUM(?) as accumulation  
FROM aggregate
