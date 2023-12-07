package com.example.weather;

import com.example.weather.DAO.Connector;
import com.example.weather.SendEmail;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.util.List;
import java.nio.file.*;
import java.util.stream.Collectors;

public class Extract {
    private static final String HOSTNAME = "localhost";
    private static final String STAGING_DB_NAME = "weather_warehouse";
    private static final String USERNAME = "root";
    private static final String PASSWORD = "";

    public Extract() {

    }

    public void extract() throws SQLException {
//        Kết nối control.db
        try (Connection configConnection = Connector.getControlConnection();) {
//      Kiểm tra kết nối có thành công hay không?
            if (configConnection.isValid(5)) {
                // Lấy dữ liệu bảng config có Flag = TRUE Status = CRAWLED
                ResultSet resultSet = Connector.getResultSetWithConfigFlags(configConnection, "TRUE", "CRAWLED");
//              Kiểm tra còn dòng config nào status= CRAWLED và FLAG = TRUE không?
                String csv_folder_path = "";
                String idConfig = "";
                while (resultSet.next()) {
                     csv_folder_path = resultSet.getString("download_path");
                     idConfig = resultSet.getString("id").trim();
                    // Cập nhật status EXTRACTING config table
                    Connector.updateStatusConfig(configConnection, idConfig, "EXTRACTING");
                    // Kết nối với wearther_warehouse db
                    try (Connection stagingConnection = Connector.getConnection(HOSTNAME, STAGING_DB_NAME, USERNAME, PASSWORD)) {
                        //      Kiểm tra kết nối có thành công hay không?
                        if (stagingConnection.isValid(5)) {
                            // Truncate bảng records_staging
                            List<String> sqlLines = Files.readAllLines(Path.of("truncate_records_staging.sql"));
                            String truncateQuery = String.join(" ", sqlLines);
                            PreparedStatement preparedStatement = configConnection.prepareStatement(truncateQuery);
                            preparedStatement.executeUpdate();
//                          Lấy danh sách các tệp tin CSV trong thư mục
                            List<Path> csvFiles = Files.list(Paths.get(csv_folder_path))
                                    .filter(path -> path.toString().endsWith(".csv"))
                                    .collect(Collectors.toList());
//                            Kiểm tra xem có file csv trong thư mục hay không?
                            if (!csvFiles.isEmpty()) {
//                               Load dữ liệu từ file csv với địa chỉ trong config vào bảng records_staging
                                processAndInsertData(stagingConnection, csvFiles);
//                          Cập nhật status EXTRACTED trong wearther_warehouse.db
                                Connector.updateStatusConfig(configConnection, idConfig, "EXTRACTED");
  //                            Đóng kết nối wearther_warehouse.db
                                stagingConnection.close();
                            } else {
                                // Cập nhật status ERR config table
                                Connector.updateStatusConfig(configConnection, idConfig, "ERR");
//			                thêm thông tin (thời gian, kết quả ) vào bảng log
                                Connector.writeLog(configConnection,
                                        "EXTRACT",
                                        "Loading data from csv files to records_staging table",
                                        idConfig,
                                        "ERR",
                                        "CSV files not exist");

//			                Gửi mail thông báo lỗi
                                String toEmail = resultSet.getString("error_to_email");
                                String subject = "Error Encountered During Data Loading Process";

                                String emailContent = "Dear Admin,\n\n"
                                        + "We regret to inform you that an error occurred during the process of loading data from CSV files to the 'records_staging' table. "
                                        + "The specific issue is as follows:\n\n"
                                        + "Error Message: Not csv files exist\n\n"
                                        + "Our technical team is actively investigating the matter and will provide a resolution as soon as possible.\n\n"
                                        + "Thank you for your understanding.\n\n"
                                        + "Best Regards,\nYour Application Team";

                                SendEmail.sendMail(toEmail, subject, emailContent);

                            }


                        } else {
                            // Cập nhật Flag=FALSE trong bảng config
                            Connector.updateFlagDataLinks(configConnection, idConfig, "FALSE");
//                            // Ghi vào log sự kiện cập nhật flag
                            Connector.writeLog(configConnection,
                                    "EXTRACT",
                                    "Loading data from csv files to records_staging table",
                                    idConfig,
                                    "ERR",
                                    "Cant connect to Staging DB");
                            // Gửi mail thông báo config không kết nối với wearther_warehouse.db được
                            String toEmail = resultSet.getString("error_to_email");
                            String subject = "Error Connecting to wearther_warehouse Database";

                            String emailContent = "Dear User,\n\n"
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
                // Xóa tất cả các tệp tin CSV sau khi xử lý xong
                clearFolder(csv_folder_path);
            }
            // Đóng kết nối control.db
            configConnection.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    public void processAndInsertData(Connection connection, List<Path> csvLines) throws SQLException, IOException {
        for (Path csv_linkString : csvLines) {
            BufferedReader reader = new BufferedReader(new FileReader(String.valueOf(csv_linkString)));
            String line;
            int lineCount = 0;

            while ((line = reader.readLine()) != null && lineCount < 24) {
                String[] data = line.split(";");

                String[] city_nameStrings = data[0].split(", ");
                String city_name = city_nameStrings[1];
                String time_record = data[1];
                String date_record = data[2];
                String time_forecast = data[3];
                String date_forecast = data[4];
                String temperature = data[5];
                String feel_like = data[6];
                String description = data[7];
                String wind_direction = data[8];
                String wind_speed = data[9];
                String humidity = data[10];
                String uv_index = data[11];
                String cloud_cover = data[12];
                String precipitation = data[13];
                String accumulation = data[14];

                // Lưu dữ liệu vào cơ sở dữ liệu
                List<String> sqlLines = Files.readAllLines(Path.of("insertDataToRecords_staging.sql"));
                System.out.println(sqlLines + "sql nè");
                String insertQuery = String.join(" ", sqlLines);

                try (PreparedStatement preparedStatement = connection.prepareStatement(insertQuery)) {
                    preparedStatement.setString(1, city_name);
                    preparedStatement.setString(2, time_record);
                    preparedStatement.setString(3, date_record);
                    preparedStatement.setString(4, time_forecast);
                    preparedStatement.setString(5, date_forecast);
                    preparedStatement.setString(6, temperature);
                    preparedStatement.setString(7, feel_like);
                    preparedStatement.setString(8, description);
                    preparedStatement.setString(9, wind_direction);
                    preparedStatement.setString(10, wind_speed);
                    preparedStatement.setString(11, humidity);
                    preparedStatement.setString(12, uv_index);
                    preparedStatement.setString(13, cloud_cover);
                    preparedStatement.setString(14, precipitation);
                    preparedStatement.setString(15, accumulation);

                    preparedStatement.executeUpdate();
                }
                lineCount++;

            }
        }

    }

    public void clearFolder(String pathString) throws IOException {
        // Xóa tất cả các tệp tin CSV trong thư mục
        Files.list(Paths.get(pathString))
                .filter(path -> path.toString().endsWith(".csv"))
                .forEach(path -> {
                    try {
                        Files.delete(path);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
    }

    public static void main(String[] args) throws IOException, SQLException {
    }
}