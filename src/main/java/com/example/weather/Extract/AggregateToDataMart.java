package com.example.weather.Extract;


import com.example.weather.DAO.Connector;
import com.example.weather.SendEmail;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class AggregateToDataMart {
	Connector connector;
	private static final String HOSTNAME = "localhost";
	private static final String STAGING_DB_NAME = "weather_warehouse";
	private static final String MART_DB_NAME = "weather_mart";
	private static final String USERNAME = "root";
	private static final String PASSWORD = "";
	public AggregateToDataMart() {
		connector = new Connector();
	}
	private static final String CONTROL_DB_URL = "jdbc:mysql://localhost:3306/control";
	private static final String STAGING_DB_URL = "jdbc:mysql://localhost:3306/staging";
	private static final String MART_DB_URL = "jdbc:mysql://localhost:3306/mart";

	public void aggregateToMart() {
		try (Connection configConnection = connector.getControlConnection()) {
			if (configConnection.isValid(5)) {
				// Step 3: Get data from config table
				String sql = readSQLFile("C:\\Users\\LAPTOP USA PRO\\Documents\\Navicat\\MySQL\\Servers\\localhost\\control\\select_Flag_Status.sql");
				// Thực thi câu SQL
				PreparedStatement preparedStatement = configConnection.prepareStatement(sql);
				ResultSet resultSet = preparedStatement.executeQuery();

				// Step 4: Update status DATAMART_LOAD config table
				int idConfig = resultSet.getInt(1);
				while (resultSet.next()) {
					connector.updateStatusConfig(configConnection, String.valueOf(idConfig), "DATAMART_LOAD");


					// Step 5: Connect to staging.db
					try (Connection stagingConnection = connector.getConnection(HOSTNAME, STAGING_DB_NAME, USERNAME, PASSWORD)) {
						if (stagingConnection.isValid(5)) {
							// Step 7.1: Connect to mart.db
							try (Connection martConnection = connector.getConnection(HOSTNAME, MART_DB_NAME, USERNAME, PASSWORD)) {
								if (martConnection.isValid(5)) {
									// Step 10: Get data from aggregate table
									// ... (your SQL query here)

									// Step 11: Transfer data to weather_hours_records
									// ... (your SQL insert or update query here)

									// Step 12: Run method to calculate averages
									// ... (call your method here)

									// Step 13: Transfer calculated data to weather_date_records
									// ... (your SQL insert or update query here)

									// Step 14: Update STATUS = DATAMART_LOADED in config table
									// ... (your SQL update query here)

								} else {
									// Step 9a: Update Flag=FALSE in config table
									// ... (your SQL update query here)

									// Step 10a: Log event
									// ... (log event code here)

									// Step 11a: Send email notification
									SendEmail.sendMail("Không thể kết nối đến Mart.db!");

									// Step 20: Close connections
									connector.closeConnectDB(martConnection);
									return; // Exit the program
								}
							}

							// Step 15: Check for remaining unprocessed configs
							// ... (your SQL query here)

							// Step 16: Log information
							// ... (log information code here)

							// Step 17: Send success email
							SendEmail.sendMail("The data mart processing was successful.");

							// Step 18: Update STATUS = PREPARED in config table
							// ... (your SQL update query here)

							// Step 19: Close connections
							connector.closeConnectDB(stagingConnection);

						} else { // Step 6.2
							// Step 9a: Update Flag=FALSE in config table
							// ... (your SQL update query here)

							// Step 10a: Log event
							// ... (log event code here)

							// Step 11a: Send email notification
							SendEmail.sendMail("Config Database Connection Failure. Unable to connect to staging.db.");

							// Step 20: Close connection
							return; // Exit the program
						}
					}
				}

			} else { // Step 2.2
				System.out.println("Connection to control.db failed.");
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	private String readSQLFile(String filePath) {
		StringBuilder content = new StringBuilder();
		try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
			String line;
			while ((line = reader.readLine()) != null) {
				content.append(line).append("\n");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return content.toString();
	}
	public static void main(String[] args) throws SQLException {
		AggregateToDataMart aggregateToDataMart = new AggregateToDataMart();
		aggregateToDataMart.aggregateToMart();
	}
}
