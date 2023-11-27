package com.example.weather.Extract;


public class AggregateToDataMart {
	ConnectDB connectDB;

	String url_control = "jdbc:mysql://localhost:3306/control"; 
	String url_mart = "jdbc:mysql://localhost:3306/weather_mart"; 
	String url_weather_warehouse = "jdbc:mysql://localhost:3306/weather_warehouse";
	public AggregateToDataMart() {
		connectDB = new ConnectDB();
	}
	public static void main(String[] args) {
		AggregateToDataMart aggregateToDataMart = new AggregateToDataMart();
		
	}
}
