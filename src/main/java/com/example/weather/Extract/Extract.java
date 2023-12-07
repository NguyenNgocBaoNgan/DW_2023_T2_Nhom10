package com.example.weather.Extract;

import com.example.weather.DAO.Connector;

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
    Connector connector;
    private static final String HOSTNAME = "localhost";
    private static final String STAGING_DB_NAME = "weather_warehouse";
    private static final String USERNAME = "root";
    private static final String PASSWORD = "";
    private static final String CSV_FOLDER_PATH = "D:\\Downloads\\Data crawl";

    public Extract() {
        connector = new Connector();
    }

    public void extract() {
        try (Connection configConnection = connector.getControlConnection()) {
            if (configConnection.isValid(5)) {
                // Lấy dữ liệu bảng config có Flag = TRUE Status = CRAWLED

//                List<String> sqlLines = Files.readAllLines(Path.of("C:\\Users\\LAPTOP USA PRO\\Documents\\Navicat\\MySQL\\Servers\\localhost\\weather_warehouse\\insertDataToRecords_staging.sql"));
//                String insertQuery = String.join(" ", sqlLines);
//                PreparedStatement preparedStatement = configConnection.prepareStatement(insertQuery);
//                preparedStatement.setString(1, "EXTRACTING");
//                preparedStatement.executeUpdate();

				// Cập nhật status EXTRACTING config table

				// kết nối với staging db
                try (Connection stagingConnection = connector.getConnection(HOSTNAME, STAGING_DB_NAME, USERNAME, PASSWORD)) {
                    if (stagingConnection.isValid(5)) {
						// truncate bảng records_staging
						List<String> sqlLines = Files.readAllLines(Path.of("C:\\Users\\LAPTOP USA PRO\\Documents\\Navicat\\MySQL\\Servers\\localhost\\weather_warehouse\\truncate_records_staging.sql"));
						String truncateQuery = String.join(" ", sqlLines);
						PreparedStatement preparedStatement = configConnection.prepareStatement(truncateQuery);
						preparedStatement.executeUpdate();
						// xử lý csv
						processCsvFiles(stagingConnection);
                    }else{
//						Cập nhật Flag=FALSE trong bảng config
//						Ghi vào log sự kiện cập nhật flag
//						Gửi mail thông báo config không kết nối với staging.db được
					}
					// Xóa tất cả các tệp tin CSV sau khi xử lý xong
					clearFolder(CSV_FOLDER_PATH);
//					Đóng kết nối records_stagging.db
					stagingConnection.close();
                }
            } else {
                System.out.println("Database connection failed.");
            }
//			Đóng kết nối control.db
            configConnection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void processCsvFiles(Connection connection) {
        try {
            // Lấy danh sách các tệp tin CSV trong thư mục
            List<Path> csvFiles = getCsvFiles();
            // Load dữ liệu từ file csv với địa chỉ trong config vào bảng records_staging
            processAndInsertData(connection, csvFiles);

        } catch (IOException | SQLException e) {
//			Cập nhật status ERR config table
//			thêm thông tin (thời gian, kết quả ) vào bảng log
//			Gửi mail thông báo lỗi
            e.printStackTrace();
        }
    }

    public List<Path> getCsvFiles() throws IOException {
        // Lọc tất cả các tệp tin có định dạng CSV trong thư mục
        return Files.list(Paths.get(CSV_FOLDER_PATH))
                .filter(path -> path.toString().endsWith(".csv"))
                .collect(Collectors.toList());
    }

    public void processAndInsertData(Connection connection, List<Path> csvLines) throws SQLException, IOException {
        for (Path csv_linkString : csvLines) {
            BufferedReader reader = new BufferedReader(new FileReader(String.valueOf(csv_linkString)));
            String line;
            int lineCount = 0;

            while ((line = reader.readLine()) != null && lineCount < 24) {
                // Skip the last 2 lines
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
//						========================================= file sql ==============================
                List<String> sqlLines = Files.readAllLines(Path.of("C:\\Users\\LAPTOP USA PRO\\Documents\\Navicat\\MySQL\\Servers\\localhost\\weather_warehouse\\insertDataToRecords_staging.sql"));
                String insertQuery = String.join(" ", sqlLines);

                try (PreparedStatement preparedStatement = connection.prepareStatement(insertQuery)) {
                    // Set parameter values
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

                    // Execute the query
                    preparedStatement.executeUpdate();
//							========================================= file sql ==============================
                }
                lineCount++;
				//Cập nhật status EXTRACTED trong config.db
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
        Extract extract = new Extract();
        Connector connection = new Connector();
//			extract.connectDB(url_control, username, password);
//			extract.extract();
//			extract.clearFolder("D:\\Downloads\\Data crawl");
    }
}