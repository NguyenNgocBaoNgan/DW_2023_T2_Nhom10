package com.example.weather.Extract;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class Extract {
	ConnectDB connectDB;

	String url_control = "jdbc:mysql://localhost:3306/control"; 
	String url_mart = "jdbc:mysql://localhost:3306/weather_mart"; 
	String url_weather_warehouse = "jdbc:mysql://localhost:3306/weather_warehouse";
																					
	public Extract() {
		connectDB = new ConnectDB();
	}

	public void extract() {
		Statement statement_control = null;
		Statement statement_staging = null;
		try {
//			ket noi control db
			Connection connection_control = connectDB.connectDB(url_control);
			statement_control = connection_control.createStatement();
//			cap nhat status EXTRACTING
			String updateQuery = "UPDATE configuration SET status = 'EXTRACTING' WHERE flag = 'TRUE' AND status = 'CRAWLED'";
			statement_control.executeUpdate(updateQuery);
//			ket noi vao staging db
			Connection connection_staging = connectDB.connectDB(url_weather_warehouse);
			if(connection_staging != null) {				
				statement_staging = connection_staging.createStatement();
			}else {
//				cap nhat trang thai FALSE
				String queryUpdateStt = "UPDATE configuration SET flag = 'FALSE' WHERE flag = 'TRUE'";
//				ghi log
				String logFalseString ="";
//				gui mail
				String mail ="";
			}
//			truncate records_staging
			String truncateQuery = "TRUNCATE TABLE records_staging";
			statement_staging.execute(truncateQuery);
//			lay path csv trong control db
			String query = "SELECT dowload_path FROM configuration";
			ResultSet resultSet_csvLink = statement_control.executeQuery(query);
			List<String> csv_LinkList = new ArrayList<String>();
//			luu link csv vao 1 list link
			while (resultSet_csvLink.next()) {
				csv_LinkList.add(resultSet_csvLink.getString(0));
			}
			try {

//			duyet qua tung link, doc tung file csv
			for (String csv_linkString : csv_LinkList) {
				BufferedReader reader = new BufferedReader(new FileReader(csv_linkString));
				if(reader.readLine() != null) {
				String line;
				while ((line = reader.readLine()) != null) {
					String[] data = line.split(";");
					
					String[] city_nameStrings = data[0].split(",");
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
				
				}else {
//					cap nhat trang thai ERR
					String queryUpdateStt = "UPDATE configuration SET status = 'ERR' WHERE status = 'EXTRACTING'";
//					ghi log ERR vào bang log
					String logQuery = "INSERT INTO logs (id,activity_type,timestamp,description,config_id,status,error_detail) VALUES (1,'Web Crawling','2023-10-21 10:15:32','Crawled data from website',1,'Success','Cannot connect to DB')";
//					gui mail
				}
//				cap nhat status EXTRACTED
				String queryUpdateStt = "UPDATE configuration SET status = 'EXTRACTED' WHERE status = 'EXTRACTING'";

				reader.close();
				statement_control.close();
				statement_staging.close();
				
				connection_control.close();
				connection_staging.close();
			}
			} catch (Exception e) {
	            System.out.println("Lỗi khi đọc file CSV!");
	            e.printStackTrace();
			}
			
		} catch (SQLException e) {
			System.out.println("Kết nối đến cơ sở dữ liệu thất bại!");
			e.printStackTrace();

		}

	}

	public static void main(String[] args) throws IOException, SQLException {
//		  Thông tin kết nối
		String url_control = "jdbc:mysql://localhost:3306/control"; // URL kết nối đến cơ sở dữ liệu control
		String url_mart = "jdbc:mysql://localhost:3306/weather_mart"; // URL kết nối đến cơ sở dữ liệu weather_mart
		String url_weather_warehouse = "jdbc:mysql://localhost:3306/weather_warehouse"; // URL kết nối đến cơ sở dữ liệu
																						// weather_warehouse
		String username = "root"; // Tên đăng nhập MySQL
		String password = ""; // Mật khẩu MySQL
		Extract extract = new Extract();
//		extract.connectDB(url_control, username, password);
		extract.extract();

	}

}
