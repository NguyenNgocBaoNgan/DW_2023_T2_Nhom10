DROP TABLE IF EXISTS `aggregate`;
CREATE TABLE IF NOT EXISTS `aggregate`(`id` int(11) NOT NULL AUTO_INCREMENT,
  `province_name` int(11) NOT NULL DEFAULT 0 COMMENT 'Province ',
  `date_record` date DEFAULT NULL,
  `time_record` time NOT NULL DEFAULT '00:00:00' COMMENT 'Recording start time',
  `time_forcast` time NOT NULL DEFAULT '00:00:00' COMMENT 'Forcast time',
  `date_forcast` int(11) NOT NULL DEFAULT 0 COMMENT 'Forcast date',
  `temperature` tinyint(4) NOT NULL DEFAULT 0 COMMENT 'Temperature',
  `feel_like` tinyint(4) NOT NULL DEFAULT 0 COMMENT 'Thermal sensation',
  `description_name` varchar(200) DEFAULT NULL COMMENT 'Desciption for weather',
  `wind_direction_name`  varchar(50) DEFAULT NULL COMMENT 'Wind direction',
  `wind_speed` smallint(5) unsigned NOT NULL DEFAULT 0 COMMENT 'Wind speed',
  `humidity` tinyint(3) unsigned NOT NULL DEFAULT 0 COMMENT 'Humidity',
  `uv_index` tinyint(3) unsigned NOT NULL DEFAULT 0 COMMENT 'UV index',
  `cloud_cover` tinyint(3) unsigned NOT NULL DEFAULT 0 COMMENT 'Cloud cover',
  `precipitation` tinyint(3) unsigned DEFAULT 0 COMMENT 'Chance of rain',
  `accumulation` float unsigned NOT NULL DEFAULT 0 COMMENT 'Amount of rain',
  PRIMARY KEY (`id`));
INSERT INTO aggregate(province_name, date_record,temperature, feel_like, description_name, wind_direction_name, wind_speed, humidity, uv_index, cloud_cover, precipitation, accumulation)
SELECT province_dim.name_vi, date_dim.full_date,records.temperature, records.feel_like, description_dim.name_vi, wind_direction_dim.name_vi, records.wind_speed, records.humidity, records.uv_index, records.cloud_cover, records.precipitation, records.accumulation
FROM records, province_dim, date_dim, description_dim, wind_direction_dim 
WHERE records.province_id = province_dim.id 
AND records.date_record = date_dim.full_date 
AND records.description_id = description_dim.id 
AND records.wind_direction_id = wind_direction_dim.id
AND isExpired = 'FALSE' 
LIMIT 1512













