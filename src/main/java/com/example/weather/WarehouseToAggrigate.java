package com.example.weather;

import com.example.weather.DAO.Connector;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class WarehouseToAggrigate {
    public WarehouseToAggrigate() {

    }

    public void whToAggrigate() throws SQLException {
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
                    // Cập nhật status AGGRIGATE_LOAD config table
                    Connector.updateStatusConfig(configConnection, idConfig, "AGGRIGATE_LOAD");

//				 // Connect to wearther_warehouse.db
                    String hostName = "localhost";
                    String dbName = resultSet.getString("WH_db_name");
                    String username = resultSet.getString("WH_source_username");
                    String password = resultSet.getString("WH_source_password");
                    try (Connection stagingConnection = Connector.getConnection(hostName, dbName, username, password)) {
                        //      Kiểm tra kết nối có thành công hay không?
                        if (stagingConnection.isValid(5)) {
                            //Truncate  aggrigate table and transfer data from records to aggrigate
                            // Đọc toàn bộ nội dung file
                            String sql = Files.readString(Path.of("insert_data_Aggrigate.sql"));
                            // Tách thành mảng các câu lệnh riêng lẻ
                            String[] commands = sql.split(";");
                            // Duyệt và thực thi từng câu lệnh
                            for (String command : commands) {
                                // Xóa ký tự xuống dòng ở đầu và cuối
                                command = command.trim();
                                if (command.isEmpty()) {
                                    continue;
                                }
                                // Thực thi câu lệnh SQL
                                try (PreparedStatement ps = stagingConnection.prepareStatement(command)) {
                                    System.out.println(command);
                                    ps.executeUpdate();
                                }
                            }
                            // Update config table status AGGRIGATE_LOADED
                            Connector.updateStatusConfig(configConnection, idConfig, "AGGRIGATE_LOADED");
                            // thêm thông tin (thời gian, kết quả ) vào bảng log
                            Connector.writeLog(configConnection,
                                    "WAREHOUSE_TO_AGGRIGATE",
                                    "Load data from warehouse to aggrigate table",
                                    idConfig,
                                    "SUCCESS",
                                    "Load data from warehouse to aggrigate table successfully!");

                            //Đóng kết nối stagging.db
                            stagingConnection.close();
                        } else {
                            //  Update config table status FALSE
                            Connector.updateFlagDataLinks(configConnection, idConfig, "FALSE");
                            // Ghi vào log sự kiện cập nhật flag
                            Connector.writeLog(configConnection,
                                    "WAREHOUSE_TO_AGGRIGATE",
                                    "Loading data from records_staging table to aggrigate table",
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

            }
//                Đóng kết nối control.db
            configConnection.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    public static void main(String[] args) throws SQLException {
        WarehouseToAggrigate whAggrigate = new WarehouseToAggrigate();
        whAggrigate.whToAggrigate();
    }
}
