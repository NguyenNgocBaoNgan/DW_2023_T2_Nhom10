package com.example.weather.Extract;

import com.example.weather.DAO.Connector;
import com.example.weather.SendEmail;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;

public class WarehouseToAggregate {
    Connector connector;
    private static final String HOSTNAME = "localhost";
    private static final String STAGING_DB_NAME = "weather_warehouse";
    private static final String USERNAME = "root";
    private static final String PASSWORD = "";

    public WarehouseToAggregate() {
        connector = new Connector();
    }

    public void whToAggregate() throws SQLException {
        // Step 1: Connect to control.db
        try (Connection configConnection = connector.getControlConnection()) {
            if (configConnection.isValid(5)) { // Step 2
                // Step 3: Get data from config table
                String sql = readSQLFile("C:\\Users\\LAPTOP USA PRO\\Documents\\Navicat\\MySQL\\Servers\\localhost\\control\\select_Flag_Status.sql");
				// Thực thi câu SQL
				PreparedStatement preparedStatement = configConnection.prepareStatement(sql);
				ResultSet resultSet = preparedStatement.executeQuery();

				// Step 4: Update status in config table
                int idConfig = resultSet.getInt(1);
                while (resultSet.next()) {
                    connector.updateStatusConfig(configConnection, String.valueOf(idConfig), "AGGREGATE_LOAD");


//				 // Step 5: Connect to staging.db
                    try (Connection stagingConnection = connector.getConnection(HOSTNAME, STAGING_DB_NAME, USERNAME, PASSWORD)) {
                        if (stagingConnection.isValid(5)) { // Step 6
                            // Step 7.1: Transfer data from records to aggregate
                            String sql_insert = readSQLFile("C:\\Users\\LAPTOP USA PRO\\Documents\\Navicat\\MySQL\\Servers\\localhost\\weather_warehouse\\insert_data_Aggregate.sql");
                            Statement statement = stagingConnection.createStatement();
                            statement.execute(sql_insert);
                            // Step 8: Update config table
                            connector.updateStatusConfig(configConnection, String.valueOf(idConfig), "AGGREGATE_LOADED");
                            // Step 9: JOIN and update data in aggregate
                            // ... (your SQL join and update query here)

                            // Step 10: Update status in config table
                            // ... (your SQL update query here)

                            // Step 11: Check for remaining unprocessed configs
                            // ... (your SQL query here)

                            // Step 12: Log information
                            connector.writeLog(configConnection,
                                    "WAREHOUSE_TO_AGGREGATE",
                                    "Load data",
                                    String.valueOf(idConfig),
                                    "SUCCESS",
                                    "");

                            // Step 13: Send success email
                            SendEmail.sendMail("Load dữ liệu từ staging vào aggregate thành công!");

                        } else { // Step 6.2
                            // Step 7.2: Update config table
                            connector.updateFlagConfig(configConnection, String.valueOf(idConfig), "FALSE");

                            // Step 8.2: Log and send email for staging.db connection failure
                            connector.writeLog(configConnection,
                                    "WAREHOUSE_TO_AGGREGATE",
                                    "Load data",
                                    String.valueOf(idConfig),
                                    "ERR",
                                    "Cannot connect to warehouse database");
                            // Step 9.2: Log and send email for config.db connection failure
//						sendEmail.sendMail("Config ID " + configId + " không kết nối với staging database");
                        }
                    }
                }
                connector.closeConnectDB(configConnection);
            } else { // Step 2.2
                System.out.println("Connection to records_staging.db failed.");
            }
        } catch (Exception e) {
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
        WarehouseToAggregate whAggregate = new WarehouseToAggregate();
        whAggregate.whToAggregate();
    }
}
