package com.example.weather.Extract;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.Statement;

public class WarehouseToAggregate {
	ConnectDB connectDB;

	public WarehouseToAggregate() {
		connectDB = new ConnectDB();
	}
	public static void main(String[] args) {
		WarehouseToAggregate whAggregate = new WarehouseToAggregate();

	}
}
