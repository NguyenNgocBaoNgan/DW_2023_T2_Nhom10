package com.example.weather;

import com.example.weather.DAO.Connector;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class Transform {

    static String folder_name = Connector.getCurrentDir();

    public void startTransform() {
        //Kết nối control.db
        //Kiểm tra kết nối có thành công hay không?
        try (Connection configConnection = Connector.getControlConnection()) {
            //Lấy dữ liệu bảng configuration có Flag = TRUE Status = EXTRACTED
            ResultSet resultSet = Connector.getResultSetWithConfigFlags(configConnection, "TRUE", "EXTRACTED");
            //Kiểm tra có dữ liệu bảng configuration hay không?
            if (resultSet.isBeforeFirst()) {
                do {
                    resultSet.next();
                    String idConfig = resultSet.getString("id").trim();
                    String recipientEmail = resultSet.getString("error_to_email").trim();
                    //Cập nhật status TRANSFORMING configuration table
                    Connector.updateStatusConfig(configConnection, idConfig, "TRANSFORMING");
                    //Kết nối với staging.db
                    String hostName = "localhost";
                    String dbName = resultSet.getString("WH_db_name");
                    String username = resultSet.getString("WH_source_username");
                    String password = resultSet.getString("WH_source_password");
                    //Kiểm tra kết nối có thành công hay không?
                    try (Connection WHConnection = Connector.getConnection(hostName, dbName, username, password)) {
                        //Kiểm tra file transform_data.sql có tồn tại hay không?
                        if (Files.exists(Path.of((folder_name + "\\transform_data.sql")))) {
                            //Chạy file transform_data.sql
                            String sqlTransform = readFileAsString(folder_name + "\\transform_data.sql");
                            String[] commands = sqlTransform.split(";");
                            PreparedStatement ps = null;
                            for (String command : commands) {
                                if (!command.isEmpty()) {
                                    System.out.println("Transforming****");
                                    if (ps != null) {
                                        ps.close();
                                    }
                                    ps = WHConnection.prepareStatement(command.trim());
                                    ps.executeUpdate();
                                }
                            }
                            if (ps != null) {
                                ps.close();
                            }


                            //Cập nhật status TRANSFORMED configuration table
                            Connector.updateStatusConfig(configConnection, idConfig, "TRANSFORMED");
                            //Ghi vào log transform thành công
                            Connector.writeLog(configConnection,
                                    "TRANSFORM",
                                    "Transform data",
                                    idConfig,
                                    "SUCCESS",
                                    "");
                        } else {
                            //Cập nhật Flag=FALSE trong bảng configuration
                            Connector.updateFlagConfig(configConnection, idConfig, "FALSE");
                            //Ghi vào log sự kiện cập nhật flag và lỗi
                            Connector.writeLog(configConnection,
                                    "TRANSFORM",
                                    "Transform data",
                                    idConfig,
                                    "ERR",
                                    "Sql file transform_data.sql does not exist or failed to access path");
                            //Gửi mail thông báo file transform_data.sql không tồn tại
                            String subject = "ERROR in Transformation Step";
                            String body = "Dear Admin,\n\n" +
                                    "We encountered an error during the transformation step of our process.\n\n" +
                                    "Error Details: The SQL file 'transform_data.sql' does not exist or there was a failure in accessing the specified path.\n\n" +
                                    "Please take the following actions to resolve the issue:\n" +
                                    "1. Check if the SQL file 'transform_data.sql' exists in the specified location.\n" +
                                    "2. Ensure that the application has the necessary permissions to access the specified path.\n\n" +
                                    "If you need further assistance, feel free to contact our support team.\n\n" +
                                    "Thank you,\nYour Application Team";
                            SendEmail.sendMail(recipientEmail, subject, body);
                        }
                        //Đóng kết nối warehouse.db
                        WHConnection.close();

                    } catch (IOException ex) {
                        //Cập nhật Flag=FALSE trong bảng configuration
                        Connector.updateFlagConfig(configConnection, idConfig, "FALSE");
                        //Ghi vào log sự kiện cập nhật flag và lỗi
                        Connector.writeLog(configConnection,
                                "TRANSFORM",
                                "Connect to warehouse database",
                                idConfig,
                                "ERR",
                                "Cannot connect to warehouse database");
                        //Gửi mail thông báo configuration không kết nối với warehouse.db được
                        String subject = "ERROR: Connection Issue with Warehouse Database";
                        String body = "Dear Admin,\n\n" +
                                "We encountered an error while trying to connect to the warehouse database.\n\n" +
                                "Error Details: Unable to establish a connection to the warehouse database.\n\n" +
                                "Please take the following actions to resolve the issue:\n" +
                                "1. Check the network connection and ensure the database server is accessible.\n" +
                                "2. Verify the database credentials used for the connection.\n\n" +
                                "If you need further assistance, feel free to contact our support team.\n\n" +
                                "Thank you,\nYour Application Team";
                        SendEmail.sendMail(recipientEmail, subject, body);
                    }
                    //Kiểm tra còn dòng configuration nào status= EXTRACTED và FLAG = TRUE không?
                } while (resultSet.next());
            }
            //Đóng kết nối control.db
            configConnection.close();
        } catch (SQLException e) {
            System.out.println(e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private String readFileAsString(String filePath) throws Exception {
        String data;
        data = new String(Files.readAllBytes(Paths.get(filePath)));
        return data;
    }


    public static void main(String[] args) throws IOException {
        Transform trans = new Transform();
        trans.startTransform();
    }
}
