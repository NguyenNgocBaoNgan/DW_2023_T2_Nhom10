package com.example.weather;

import com.example.weather.DAO.Connector;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class AggrigateToDataMart {
    public AggrigateToDataMart() {
    }

    public void aggrigateToMart() {
        // Connect to control.db
        try (Connection configConnection = Connector.getControlConnection()) {
            //      Kiểm tra kết nối có thành công hay không?
            if (configConnection.isValid(5)) {
                // Lấy dữ liệu có trong bảng config Flag = TRUE Status = AGGRIGATE_LOADED
                ResultSet resultSet = Connector.getResultSetWithConfigFlags(configConnection, "TRUE", "AGGRIGATE_LOADED");

                //Kiểm tra có còn dòng config nào  không?
                while (resultSet.next()) {
                    String idConfig = resultSet.getString("id").trim();
                    //  Update status DATAMART_LOAD config table
                    Connector.updateStatusConfig(configConnection, idConfig, "DATAMART_LOAD");
                    //  Connect to weather_warehouse.db
                    String hostName = "localhost";
                    String dbName = resultSet.getString("WH_db_name");
                    String username = resultSet.getString("WH_source_username");
                    String password = resultSet.getString("WH_source_password");
                    try (Connection stagingConnection = Connector.getConnection(hostName, dbName, username, password)) {
                        //      Kiểm tra kết nối có thành công hay không?
                        if (stagingConnection.isValid(5)) {
                            // Connect to mart.db
                            String hostName1 = "localhost";
                            String dbName1 = resultSet.getString("MART_db_name");
                            String username1 = resultSet.getString("MART_source_username");
                            String password1 = resultSet.getString("MART_source_password");
                            try (Connection martConnection = Connector.getConnection(hostName1, dbName1, username1, password1)) {
                                //      Kiểm tra kết nối có thành công hay không?
                                if (martConnection.isValid(5)) {
                                    // Transfer data to weather_hours_records
                                    String insertSql = Connector.readFileAsString("insert_weather_hour_records.sql");
                                    PreparedStatement psInsert = martConnection.prepareStatement(insertSql);

                                    psInsert.executeUpdate();

                                    // Update is_available = TRUE
                                    String update_is_available = Connector.readFileAsString("update_is_available_true_hour.sql");
                                    PreparedStatement psUpdate_available = martConnection.prepareStatement(update_is_available);
                                    psUpdate_available.setString(1, "TRUE");
                                    psUpdate_available.executeUpdate();

                                    // Transfer calculated data to weather_date_records
                                    String insertSql_day_records = Connector.readFileAsString("insert_weather_day_records.sql");
                                    PreparedStatement psInsert_day_records = martConnection.prepareStatement(insertSql_day_records);

                                    psInsert_day_records.executeUpdate();

                                    // Update is_available = TRUE
                                    update_is_available = Connector.readFileAsString("update_is_available_true_day.sql");
                                    PreparedStatement psUpdate_available2 = martConnection.prepareStatement(update_is_available);
                                    psUpdate_available2.setString(1, "TRUE");
                                    psUpdate_available2.executeUpdate();

                                    // Update STATUS = DATAMART_LOADED in config table
                                    Connector.updateStatusConfig(configConnection, idConfig, "DATAMART_LOADED");
//									Đóng kết nối weather_mart.db
                                    martConnection.close();
                                    //  Đóng kết nối weather_warehouse.db
                                    stagingConnection.close();

                                    // Log information
                                    Connector.writeLog(configConnection,
                                            "AGGRIGATE_TO_DATAMART",
                                            "Loading data from csv files to records_staging table",
                                            idConfig,
                                            "SUCCESS",
                                            "Loading data from csv files to records_staging table successfully!");

                                    // Update STATUS = PREPARED in config table
                                    Connector.updateStatusConfig(configConnection, idConfig, "PREPARED");
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

                                } else {
                                    // Update Flag=FALSE in config table
                                    Connector.updateFlagDataLinks(martConnection, idConfig, "FALSE");

                                    //  Log event
                                    Connector.writeLog(configConnection,
                                            "AGGRIGATE_TO_DATAMART",
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

                                    // Update STATUS = PREPARED in config table
                                    Connector.updateStatusConfig(configConnection, idConfig, "PREPARED");
                                }
                            }

                        } else {
                            // Update Flag=FALSE in config table
                            Connector.updateFlagDataLinks(configConnection, idConfig, "FALSE");

                            // Log event Can't connect to weather_warehouse DB!
                            Connector.writeLog(configConnection,
                                    "AGGRIGATE_TO_DATAMART",
                                    "Loading data from csv files to records_staging table",
                                    idConfig,
                                    "ERR",
                                    "Can't connect to weather_warehouse DB!");

                            // Send email notification
                            String toEmail = resultSet.getString("error_to_email");
                            String subject = "Error Connecting to weather_warehouse Database";

                            String emailContent = "Dear Admin,\n\n"
                                    + "We are experiencing difficulties connecting to the weather_warehouse Database for the data loading process. "
                                    + "Unfortunately, the connection attempt has failed with the following error:\n\n"
                                    + "Error Message: Can't connect to weather_warehouse DB\n\n"
                                    + "Our technical team is actively investigating the issue and working towards a swift resolution. "
                                    + "We appreciate your patience and understanding during this time.\n\n"
                                    + "Thank you for your cooperation.\n\n"
                                    + "Best Regards,\nYour Application Team";

                            SendEmail.sendMail(toEmail, subject, emailContent);
                        }
                    }
                }
            }
//            Close connection control db
            configConnection.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) throws SQLException {
        AggrigateToDataMart aggrigateToDataMart = new AggrigateToDataMart();
        aggrigateToDataMart.aggrigateToMart();
    }
}
