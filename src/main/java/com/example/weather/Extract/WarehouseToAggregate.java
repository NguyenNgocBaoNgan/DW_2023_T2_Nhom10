package com.example.weather.Extract;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.Statement;

public class WarehouseToAggregate {
	ConnectDB connectDB;

	String url_control = "jdbc:mysql://localhost:3306/control"; 
	String url_mart = "jdbc:mysql://localhost:3306/weather_mart"; 
	String url_weather_warehouse = "jdbc:mysql://localhost:3306/weather_warehouse";
	public WarehouseToAggregate() {
		connectDB = new ConnectDB();
	}
	public void whToAgg() {
		Statement statement_staging = null;
		String csvFilePath = "D:\\Downloads\\Data crawl\\Crawl_Bac Can, Bắc Kạn_08_56_49_24-11-2023.csv";
//		ket noi vao staging db
        try {
        	Connection connection_staging = connectDB.connectDB(url_weather_warehouse);
        	statement_staging = connection_staging.createStatement();
        	BufferedReader reader = new BufferedReader(new FileReader(csvFilePath));
			String line;
			while ((line = reader.readLine()) != null) {
				String[] data = line.split(";");
				
				String[] city_nameStrings = data[0].split(", ");
				String city_name = city_nameStrings[1];
				String time_record = data[1];
				String date_record = data[2];
				String time_forecast = data[3];
				String date_forecast = data[4];
				String temperature = data[5];
				String feel_like = data[6];
				String description = data[7];
				String wind_direction = data[8];
				String wind_speed = data[9];
				String humidity = data[10];
				String uv_index = data[11];
				String cloud_cover = data[12];
				String precipitation = data[13];
				String accumulation = data[14];

				// Lưu dữ liệu vào cơ sở dữ liệu
				String insertQuery = "INSERT INTO records_staging (province, time_record, date_record, time_forcast, date_forcast, temperature, feel_like, description, wind_direction, wind_speed, humidity, uv_index, cloud_cover, precipitation, accumulation)"
						+ " VALUES ("+"'" + city_name + "', '" + time_record + "', '" + date_record
						+ "', '" + time_forecast + "', '" + date_forecast + "', " + "'" + temperature + "', '"
						+ feel_like + "', '" + description + "', '" + wind_direction + "', '" + wind_speed + "', '"
						+ humidity + "'," + " '" + uv_index + "', '" + cloud_cover + "', '" + precipitation + "', '"
						+ accumulation + "')";
				statement_staging.execute(insertQuery);
			}
            reader.close();
            statement_staging.close();
            connectDB.closeConnectDB(connection_staging);
            
        } catch (Exception e) {
            System.out.println("Lỗi khi đọc file CSV!");
            e.printStackTrace();
        }
	}
	public static void main(String[] args) {
		WarehouseToAggregate whAggregate = new WarehouseToAggregate();
		whAggregate.whToAgg();
	        
	}
}
