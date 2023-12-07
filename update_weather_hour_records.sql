UPDATE weather_hour_records w INNER JOIN aggregate a ON a.time_forcast = ? AND a.date_forcast = ? 
SET w.temperature = ?, w.feel_like = ?, w.description = ?, w.wind_direction = ?, 
w.wind_speed = ?, w.humidity = ?, w.uv_index = ?, w.cloud_cover = ?, 
w.precipitation = ?, w.accumulation = ?;