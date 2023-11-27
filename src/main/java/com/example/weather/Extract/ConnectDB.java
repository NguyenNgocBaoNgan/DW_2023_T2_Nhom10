package com.example.weather.Extract;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class ConnectDB {
//	  Thông tin kết nối
	String url_control = "jdbc:mysql://localhost:3306/control"; 
	String url_mart = "jdbc:mysql://localhost:3306/weather_mart"; 
	String url_weather_warehouse = "jdbc:mysql://localhost:3306/weather_warehouse"; 
																					
	String username = "root"; 
	String password = "";

	public ConnectDB() {
		
	}
	public Connection connectDB(String url) {
		Connection connection = null;
		try {
			connection = DriverManager.getConnection(url, username, password);
			System.out.println("Kết nối thành công đến cơ sở dữ liệu!");
			connection.close();
			System.out.println("Đã đóng kết nối đến cơ sở dữ liệu!");
		} catch (SQLException e) {
			System.out.println("Kết nối đến cơ sở dữ liệu thất bại!");
			e.printStackTrace();
		}
		return connection;

	}
	public void closeConnectDB(Connection cc) {
		try {
			cc.close();
			System.out.println("Đã đóng kết nối đến cơ sở dữ liệu!");
		} catch (SQLException e) {
			System.out.println("Kết nối đến cơ sở dữ liệu thất bại!");
			e.printStackTrace();
		}
		
	}
	public static void main(String[] args) {

		String url_control = "jdbc:mysql://localhost:3306/control"; 
		String url_mart = "jdbc:mysql://localhost:3306/weather_mart"; 
		String url_weather_warehouse = "jdbc:mysql://localhost:3306/weather_warehouse";
		ConnectDB connectDB = new ConnectDB();
		connectDB.connectDB(url_control);
	}
}
