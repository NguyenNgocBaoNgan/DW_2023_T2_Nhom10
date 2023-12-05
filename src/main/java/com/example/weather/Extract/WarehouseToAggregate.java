package com.example.weather.Extract;

import com.example.weather.DAO.Connector;
import com.example.weather.SendEmail;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.*;
import java.util.List;

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
                // Lấy dữ liệu có trong bảng config Flag = TRUE Status = PREPARED
                List<String> sqlLines = Files.readAllLines(Path.of("C:\\Users\\LAPTOP USA PRO\\Documents\\Navicat\\MySQL\\Servers\\localhost\\control\\select_Flag_Status.sql"));
                String selectQuery = String.join(" ", sqlLines);
                PreparedStatement preparedStatement = configConnection.prepareStatement(selectQuery);
                preparedStatement.setString(1, "TRUE");
                preparedStatement.setString(2, "PREPARED");
                ResultSet resultSet = preparedStatement.executeQuery();

				// Cập nhật status AGGREGATE_LOAD config table
                int idConfig = resultSet.getInt(1);
                while (resultSet.next()) {
                    connector.updateStatusConfig(configConnection, String.valueOf(idConfig), "AGGREGATE_LOAD");

//				 // Step 5: Connect to staging.db
                    try (Connection stagingConnection = connector.getConnection(HOSTNAME, STAGING_DB_NAME, USERNAME, PASSWORD)) {
                        if (stagingConnection.isValid(5)) { // Step 6
                            // Step 7.1: Transfer data from records to aggregate
                            List<String> sqlLines2 = Files.readAllLines(Path.of("C:\\Users\\LAPTOP USA PRO\\Documents\\Navicat\\MySQL\\Servers\\localhost\\weather_warehouse\\insert_data_Aggregate.sql"));
                            String insertQuery = String.join(" ", sqlLines2);
                            preparedStatement = configConnection.prepareStatement(insertQuery);
                            preparedStatement.executeUpdate();
                            // Step 8: Update config table
                            connector.updateStatusConfig(configConnection, String.valueOf(idConfig), "AGGREGATE_LOADED");

                            // Step 12: thêm thông tin (thời gian, kết quả ) vào bảng log
                            connector.writeLog(configConnection,
                                    "WAREHOUSE_TO_AGGREGATE",
                                    "Load data",
                                    String.valueOf(idConfig),
                                    "SUCCESS",
                                    "");

                            // Step 13: Gửi mail thông báo và cập nhật thành công
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
                            // Step 9.2: Gửi mail thông báo config không kết nối với staging.db được
						SendEmail.sendMail("Config ID " + idConfig + " không kết nối với staging database");
                        }
//                        Đóng kết nối stagging.db
                        connector.closeConnectDB(stagingConnection);
                    }
                }

            } else { // Step 2.2
                System.out.println("Connection to records_staging.db failed.");
            }
//                Đóng kết nối config.db
            connector.closeConnectDB(configConnection);
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
