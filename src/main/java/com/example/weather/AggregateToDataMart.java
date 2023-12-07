package com.example.weather;

import com.example.weather.DAO.Connector;
import com.example.weather.SendEmail;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public class AggregateToDataMart {
    private static final String HOSTNAME = "localhost";
    private static final String STAGING_DB_NAME = "weather_warehouse";
    private static final String MART_DB_NAME = "weather_mart";
    private static final String USERNAME = "root";
    private static final String PASSWORD = "";

    public AggregateToDataMart() {
    }

    public void aggregateToMart() {
        // Connect to control.db
        try (Connection configConnection = Connector.getControlConnection()) {
            //      Kiểm tra kết nối có thành công hay không?
            if (configConnection.isValid(5)) {
                // Lấy dữ liệu có trong bảng config Flag = TRUE Status = AGGRIGATE_LOADED
                ResultSet resultSet = Connector.getResultSetWithConfigFlags(configConnection, "TRUE", "AGGRIGATE_LOADED");

                String idConfig = resultSet.getString("id").trim();
                //                Kiểm tra còn dòng config nào chưa chạy không?
                while (resultSet.next()) {
                    //  Update status DATAMART_LOAD config table
                    Connector.updateStatusConfig(configConnection, idConfig, "DATAMART_LOAD");
                    //  Connect to staging.db
                    try (Connection stagingConnection = Connector.getConnection(HOSTNAME, STAGING_DB_NAME, USERNAME, PASSWORD)) {
                        //      Kiểm tra kết nối có thành công hay không?
                        if (stagingConnection.isValid(5)) {
                            // Connect to mart.db
                            try (Connection martConnection = Connector.getConnection(HOSTNAME, MART_DB_NAME, USERNAME, PASSWORD)) {
                                //      Kiểm tra kết nối có thành công hay không?
                                if (martConnection.isValid(5)) {
                                    // Get data from aggregate table
                                    List<String> sqlGetData = Files.readAllLines(Path.of("getDataFromAggregate.sql"));
                                    String selectDataQuery = String.join(" ", sqlGetData);
                                    PreparedStatement preparedStatement = stagingConnection.prepareStatement(selectDataQuery);
                                    ResultSet resultSet_agg = preparedStatement.executeQuery();
//                                    Kiểm tra còn dòng dữ liệu nào trong aggregate table chưa chạy không?
                                    while (resultSet_agg.next()) {
                                        // Transfer data to weather_hours_records
                                        String insertSql = Connector.readFileAsString("insert_weather_hour_records.sql");
                                        PreparedStatement psInsert = martConnection.prepareStatement(insertSql);
                                        String updateSql = Connector.readFileAsString("update_weather_hour_records.sql");
                                        PreparedStatement psUpdate = martConnection.prepareStatement(updateSql);

                                        // Insert statement
                                        psInsert.setString(1, resultSet_agg.getString("province_name"));
                                        psInsert.setTime(2, resultSet_agg.getTime("time_record"));
                                        psInsert.setDate(3, resultSet_agg.getDate("date_record"));
                                        psInsert.setTime(4, resultSet_agg.getTime("time_forcast"));
                                        psInsert.setDate(5, resultSet_agg.getDate("date_forcast"));
                                        psInsert.setInt(6, resultSet_agg.getInt("temperature"));
                                        psInsert.setInt(7, resultSet_agg.getInt("feel_like"));
                                        psInsert.setString(8, resultSet_agg.getString("description_name"));
                                        psInsert.setString(9, resultSet_agg.getString("wind_direction_name"));
                                        psInsert.setInt(10, resultSet_agg.getInt("wind_speed"));
                                        psInsert.setInt(11, resultSet_agg.getInt("humidity"));
                                        psInsert.setInt(12, resultSet_agg.getInt("uv_index"));
                                        psInsert.setInt(13, resultSet_agg.getInt("cloud_cover"));
                                        psInsert.setInt(14, resultSet_agg.getInt("precipitation"));
                                        psInsert.setFloat(15, resultSet_agg.getFloat("accumulation"));
                                        psInsert.setString(15, "FALSE");

                                        psInsert.setTime(17, resultSet_agg.getTime("time_forcast"));
                                        psInsert.setDate(18, resultSet_agg.getDate("date_forcast"));

                                        psInsert.executeUpdate();

                                        // Update statement if duplicate data
                                        psUpdate.setTime(1, resultSet_agg.getTime("time_forcast"));
                                        psUpdate.setDate(2, resultSet_agg.getDate("date_forcast"));
                                        psUpdate.setInt(3, resultSet_agg.getInt("temperature"));
                                        psUpdate.setInt(4, resultSet_agg.getInt("feel_like"));
                                        psUpdate.setString(5, resultSet_agg.getString("description_name"));
                                        psUpdate.setString(6, resultSet_agg.getString("wind_direction_name"));
                                        psUpdate.setInt(7, resultSet_agg.getInt("wind_speed"));
                                        psUpdate.setInt(8, resultSet_agg.getInt("humidity"));
                                        psUpdate.setInt(9, resultSet_agg.getInt("uv_index"));
                                        psUpdate.setInt(10, resultSet_agg.getInt("cloud_cover"));
                                        psUpdate.setInt(11, resultSet_agg.getInt("precipitation"));
                                        psUpdate.setFloat(12, resultSet_agg.getFloat("accumulation"));
                                        psUpdate.executeUpdate();

                                        // Update is_available = TRUE
                                        String update_is_available = Connector.readFileAsString("update_is_available_true.sql");
                                        PreparedStatement psUpdate_available = martConnection.prepareStatement(update_is_available);
                                        psUpdate_available.setString(1, "TRUE");
                                        psUpdate_available.executeUpdate();

                                        // Step: Transfer calculated data to weather_date_records
                                        String insertSql_day_records = Connector.readFileAsString("insert_weather_day_records.sql");
                                        PreparedStatement psInsert_day_records = martConnection.prepareStatement(insertSql_day_records);

                                        String updateSql_day_records = Connector.readFileAsString("update_weather_day_records.sql");
                                        PreparedStatement psUpdate_day_records = martConnection.prepareStatement(updateSql_day_records);

                                        // Insert statement
                                        psInsert_day_records.setString(1, resultSet_agg.getString("province_name"));
                                        psInsert_day_records.setDate(2, resultSet_agg.getDate("date_record"));
                                        psInsert_day_records.setTime(3, resultSet_agg.getTime("time_record"));
                                        psInsert_day_records.setDate(4, resultSet_agg.getDate("date_forcast"));
                                        psInsert_day_records.setInt(5, resultSet_agg.getInt("temperature"));
                                        psInsert_day_records.setInt(6, resultSet_agg.getInt("feel_like"));
                                        psInsert_day_records.setString(7, resultSet_agg.getString("description_name"));
                                        psInsert_day_records.setString(8, resultSet_agg.getString("description_name"));
                                        psInsert_day_records.setString(9, resultSet_agg.getString("description_name"));
                                        psInsert_day_records.setInt(10, resultSet_agg.getInt("humidity"));
                                        psInsert_day_records.setInt(11, resultSet_agg.getInt("cloud_cover"));
                                        psInsert_day_records.setInt(12, resultSet_agg.getInt("precipitation"));
                                        psInsert_day_records.setInt(13, resultSet_agg.getInt("accumulation"));
                                        psInsert_day_records.executeUpdate();

                                        // Update statement if duplicate data
                                        psUpdate_day_records.setTime(1, resultSet_agg.getTime("time_forcast"));
                                        psUpdate_day_records.setDate(2, resultSet_agg.getDate("date_forcast"));
                                        psUpdate_day_records.setInt(3, resultSet_agg.getInt("temperature"));
                                        psUpdate_day_records.setInt(4, resultSet_agg.getInt("feel_like"));
                                        psUpdate_day_records.setString(5, resultSet_agg.getString("description_name"));
                                        psUpdate_day_records.setString(6, resultSet_agg.getString("wind_direction_name"));
                                        psUpdate_day_records.setInt(7, resultSet_agg.getInt("wind_speed"));
                                        psUpdate_day_records.setInt(8, resultSet_agg.getInt("humidity"));
                                        psUpdate_day_records.setInt(9, resultSet_agg.getInt("uv_index"));
                                        psUpdate_day_records.setInt(10, resultSet_agg.getInt("cloud_cover"));
                                        psUpdate_day_records.setInt(11, resultSet_agg.getInt("precipitation"));
                                        psUpdate_day_records.setFloat(12, resultSet_agg.getFloat("accumulation"));
                                        psUpdate_day_records.executeUpdate();

                                        // Update STATUS = DATAMART_LOADED in config table
                                        Connector.updateStatusConfig(configConnection, idConfig, "DATAMART_LOADED");

                                    }
//									đóng kết nối mart db
                                    martConnection.close();
                                    //  Close staging db connections
                                    stagingConnection.close();
                                } else {
                                    // Update Flag=FALSE in config table
                                    Connector.updateFlagDataLinks(martConnection, idConfig, "FALSE");

                                    //  Log event
                                    Connector.writeLog(configConnection,
                                            "AGGREGATE_TO_DATAMART",
                                            "Loading data from csv files to records_staging table",
                                            idConfig,
                                            "ERR",
                                            "Cant connect to Mart DB");

                                    //  Send email notification

                                    String toEmail = resultSet.getString("error_to_email");

                                    String subject = "Connection Error to Mart.db";
                                    String emailContent = "Dear Admin,\n\n"
                                            + "We regret to inform you that an issue occurred while attempting to connect to the Mart.db database. "
                                            + "Below are the details of the error:\n\n"
                                            + "Error Message: Cannot connect to Mart.db\n\n"
                                            + "Our technical team is actively investigating the matter and will strive to resolve the issue as soon as possible.\n\n"
                                            + "Thank you for bringing this to our attention. If you require further assistance or have any questions, please feel free to contact us.\n\n"
                                            + "Best Regards,\nSupport Team";

                                    SendEmail.sendMail(toEmail, subject, emailContent);

                                }
                            }

                        } else {
                            // Update Flag=FALSE in config table
                            Connector.updateFlagDataLinks(configConnection, idConfig, "FALSE");

                            // Log event Can't connect to Staging DB!
                            Connector.writeLog(configConnection,
                                    "AGGREGATE_TO_DATAMART",
                                    "Loading data from csv files to records_staging table",
                                    idConfig,
                                    "ERR",
                                    "Can't connect to Staging DB!");

                            // Send email notification
                            String toEmail = resultSet.getString("error_to_email");
                            String subject = "Error Connecting to Staging Database";

                            String emailContent = "Dear Admin,\n\n"
                                    + "We are experiencing difficulties connecting to the Staging Database for the data loading process. "
                                    + "Unfortunately, the connection attempt has failed with the following error:\n\n"
                                    + "Error Message: Can't connect to Staging DB\n\n"
                                    + "Our technical team is actively investigating the issue and working towards a swift resolution. "
                                    + "We appreciate your patience and understanding during this time.\n\n"
                                    + "Thank you for your cooperation.\n\n"
                                    + "Best Regards,\nYour Application Team";

                            SendEmail.sendMail(toEmail, subject, emailContent);
                        }
                    }
                }
                // Log information
                Connector.writeLog(configConnection,
                        "AGGREGATE_TO_DATAMART",
                        "Loading data from csv files to records_staging table",
                        idConfig,
                        "SUCCESS",
                        "Loading data from csv files to records_staging table successfully!");

                // Send success email
                String toEmail = resultSet.getString("error_to_email");

                String subject = "Successful Data Mart Processing";

                String emailContent = "Dear Admin,\n\n"
                        + "We are pleased to inform you that the data mart processing has been completed successfully. "
                        + "All data has been processed and updated in the Mart.db database without any issues.\n\n"
                        + "Summary of the Process:\n"
                        + "- Source: [Your Source]\n"  // Replace with your actual data source
                        + "- Destination: Mart.db\n"
                        + "- Status: Success\n\n"
                        + "If you have any questions or require further information, please feel free to contact our support team.\n\n"
                        + "Thank you for your continued use of our services.\n\n"
                        + "Best Regards,\nYour Application Team";

                SendEmail.sendMail(toEmail, subject, emailContent);

                // Update STATUS = PREPARED in config table
                Connector.updateStatusConfig(configConnection, idConfig, "PREPARED");

            }
//            Close connection control db
            configConnection.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) throws SQLException {
//        AggregateToDataMart aggregateToDataMart = new AggregateToDataMart();
//        aggregateToDataMart.aggregateToMart();
    }
}
