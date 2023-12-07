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
    private static final String HOSTNAME = "localhost";
    private static final String STAGING_DB_NAME = "weather_warehouse";
    private static final String USERNAME = "root";
    private static final String PASSWORD = "";

    public WarehouseToAggregate() {

    }

    public void whToAggregate() throws SQLException {
        // Step 1: Connect to control.db
        try (Connection configConnection = Connector.getControlConnection()) {
            if (configConnection.isValid(5)) { // Step 2
                // Lấy dữ liệu có trong bảng config Flag = TRUE Status = PREPARED
                ResultSet resultSet = Connector.getResultSetWithConfigFlags(configConnection, "TRUE", "PREPARED");

				// Cập nhật status AGGREGATE_LOAD config table
                String idConfig = resultSet.getString("id").trim();
                while (resultSet.next()) {
                    Connector.updateStatusConfig(configConnection, idConfig, "AGGREGATE_LOAD");

//				 // Step 5: Connect to staging.db
                    try (Connection stagingConnection = Connector.getConnection(HOSTNAME, STAGING_DB_NAME, USERNAME, PASSWORD)) {
                        if (stagingConnection.isValid(5)) { // Step 6
                            // Step 7.1: Transfer data from records to aggregate
                            List<String> sqlLines2 = Files.readAllLines(Path.of("insert_data_Aggregate.sql"));
                            String insertQuery = String.join(" ", sqlLines2);
                            PreparedStatement preparedStatement = configConnection.prepareStatement(insertQuery);
                            preparedStatement.executeUpdate();
                            // Step 8: Update config table
                            Connector.updateStatusConfig(configConnection, idConfig, "AGGREGATE_LOADED");

                        } else { // Step 6.2
                            // Step 7.2: Update config table
                            Connector.updateFlagConfig(configConnection, idConfig, "FALSE");

                            // Step 8.2: Log and send email for staging.db connection failure
                            Connector.writeLog(configConnection,
                                    "WAREHOUSE_TO_AGGREGATE",
                                    "Load data",
                                    idConfig,
                                    "ERR",
                                    "Cannot connect to warehouse database");
                            // Step 9.2: Gửi mail thông báo config không kết nối với staging.db được
//						SendEmail.sendMail("Config ID " + idConfig + " không kết nối với staging database");
                        }
//                        Đóng kết nối stagging.db
                        stagingConnection.close();
                    }
                }
                // Step 12: thêm thông tin (thời gian, kết quả ) vào bảng log
                Connector.writeLog(configConnection,
                        "WAREHOUSE_TO_AGGREGATE",
                        "Load data",
                        idConfig,
                        "SUCCESS",
                        "");

                // Step 13: Gửi mail thông báo và cập nhật thành công
//                SendEmail.sendMail("Load dữ liệu từ staging vào aggregate thành công!");

            } else { // Step 2.2
                System.out.println("Connection to records_staging.db failed.");
            }
//                Đóng kết nối config.db
          configConnection.close();
        } catch (Exception e) {
			e.printStackTrace();
        }
    }




    public static void main(String[] args) throws SQLException {
        WarehouseToAggregate whAggregate = new WarehouseToAggregate();
        whAggregate.whToAggregate();
    }
}
