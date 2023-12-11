package com.example.weather;

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
            //      Kiểm tra kết nối có thành công hay không?
            if (configConnection.isValid(5)) {
                // Lấy dữ liệu có trong bảng config Flag = TRUE Status = PREPARED
                ResultSet resultSet = Connector.getResultSetWithConfigFlags(configConnection, "TRUE", "PREPARED");
                String idConfig = "";
//                Kiểm tra còn dòng config nào chưa chạy không?
                while (resultSet.next()) {
                    idConfig = resultSet.getString("id").trim();
                    // Cập nhật status AGGREGATE_LOAD config table
                    Connector.updateStatusConfig(configConnection, idConfig, "AGGREGATE_LOAD");

//				 // Connect to wearther_warehouse.db
                    try (Connection stagingConnection = Connector.getConnection(HOSTNAME, STAGING_DB_NAME, USERNAME, PASSWORD)) {
                        //      Kiểm tra kết nối có thành công hay không?
                        if (stagingConnection.isValid(5)) {
                            //Delete Aggregate if existed then create aggregate table and transfer data from records to aggregate
                            List<String> sqlLines2 = Files.readAllLines(Path.of("insert_data_Aggregate.sql"));
                            String insertQuery = String.join(" ", sqlLines2);
                            PreparedStatement preparedStatement = configConnection.prepareStatement(insertQuery);
                            preparedStatement.executeUpdate();
                            // Update config table status AGGREGATE_LOADED
                            Connector.updateStatusConfig(configConnection, idConfig, "AGGREGATE_LOADED");
//                        Đóng kết nối weather_warehouse.db
                            stagingConnection.close();
                        } else {
                            //  Update config table status FALSE
                            Connector.updateFlagDataLinks(configConnection, idConfig, "FALSE");
                            // Ghi vào log sự kiện cập nhật flag
                            Connector.writeLog(configConnection,
                                    "WAREHOUSE_TO_AGGREGATE",
                                    "Loading data from records_staging table to aggregate table",
                                    idConfig,
                                    "ERR",
                                    "Cant connect to wearther_warehouse DB");
                            // Gửi mail thông báo config không kết nối với wearther_warehouse.db được
                            String toEmail = resultSet.getString("error_to_email");
                            String subject = "Error Connecting to wearther_warehouse Database";

                            String emailContent = "Dear Admin,\n\n"
                                    + "We are experiencing difficulties connecting to the wearther_warehouse Database for the data loading process. "
                                    + "Unfortunately, the connection attempt has failed with the following error:\n\n"
                                    + "Error Message: Can't connect to wearther_warehouse DB\n\n"
                                    + "Our technical team is actively investigating the issue and working towards a swift resolution. "
                                    + "We appreciate your patience and understanding during this time.\n\n"
                                    + "Thank you for your cooperation.\n\n"
                                    + "Best Regards,\nYour Application Team";

                            SendEmail.sendMail(toEmail, subject, emailContent);
                        }


                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
                // thêm thông tin (thời gian, kết quả ) vào bảng log
                Connector.writeLog(configConnection,
                        "WAREHOUSE_TO_AGGREGATE",
                        "Load data from warehouse to aggregate table",
                        idConfig,
                        "SUCCESS",
                        "Load data from warehouse to aggregate table successfully!");

                // Gửi mail thông báo và cập nhật thành công
                String toEmail = resultSet.getString("success_to_email");

                String subject = "Successful Data Load from Warehouse to Aggregate Table";

                String emailContent = "Dear Admin,\n\n"
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

            }
//                Đóng kết nối control.db
            configConnection.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    public static void main(String[] args) throws SQLException {
        WarehouseToAggregate whAggregate = new WarehouseToAggregate();
        whAggregate.whToAggregate();
    }
}
