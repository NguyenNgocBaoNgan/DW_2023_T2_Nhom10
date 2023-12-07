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
//                        Đóng kết nối stagging.db
                            stagingConnection.close();
                        } else { // Step 6.2
                            // Step 7.2: Update config table
                            Connector.updateFlagDataLinks(configConnection, idConfig, "FALSE");
//                            // Ghi vào log sự kiện cập nhật flag
                            Connector.writeLog(configConnection,
                                    "EXTRACT",
                                    "Loading data from csv files to records_staging table",
                                    idConfig,
                                    "ERR",
                                    "Cant connect to Staging DB");
                            // Step 9.2: Gửi mail thông báo config không kết nối với staging.db được
                            String toEmail = resultSet.getString("error_to_email");
                            String subject = "Error Connecting to Staging Database";

                            String emailContent = "Dear User,\n\n"
                                    + "We are experiencing difficulties connecting to the Staging Database for the data loading process. "
                                    + "Unfortunately, the connection attempt has failed with the following error:\n\n"
                                    + "Error Message: Can't connect to Staging DB\n\n"
                                    + "Our technical team is actively investigating the issue and working towards a swift resolution. "
                                    + "We appreciate your patience and understanding during this time.\n\n"
                                    + "Thank you for your cooperation.\n\n"
                                    + "Best Regards,\nYour Application Team";

                            SendEmail.sendMail(toEmail, subject, emailContent);
                        }


                        // Step 12: thêm thông tin (thời gian, kết quả ) vào bảng log
                        Connector.writeLog(configConnection,
                                "WAREHOUSE_TO_AGGREGATE",
                                "Load data from warehouse to aggregate table",
                                idConfig,
                                "SUCCESS",
                                "");

                        // Step 13: Gửi mail thông báo và cập nhật thành công
                        String toEmail = resultSet.getString("success_to_email");

                        String subject = "Successful Data Load from Warehouse to Aggregate Table";

                        String emailContent = "Dear User,\n\n"
                                + "We are pleased to inform you that the process of loading data from the warehouse to the aggregate table has been completed successfully. "
                                + "All data has been aggregated and updated in the target table without any issues.\n\n"
                                + "Summary of the Process:\n"
                                + "- Source: Warehouse\n"
                                + "- Destination: Aggregate Table\n"
                                + "- Status: Success\n\n"
                                + "If you have any questions or require further information, please feel free to contact our support team.\n\n"
                                + "Thank you for your continued use of our services.\n\n"
                                + "Best Regards,\nYour Application Team";

                        SendEmail.sendMail(toEmail, subject, emailContent);

                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            } else { // Step 2.2
                return;
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
