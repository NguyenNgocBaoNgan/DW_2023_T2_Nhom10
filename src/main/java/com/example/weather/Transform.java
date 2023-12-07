package com.example.weather;

import com.example.weather.DAO.Connector;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

public class Transform {
    String FILE_LOCATION;

    public void startTransform() {
        Connector connection = new Connector();
        try (Connection configConnection = connection.getControlConnection()) {
            String getConfig = readFileAsString("get_config.sql");
            try (PreparedStatement preparedStatement = configConnection.prepareStatement(getConfig)) {
                preparedStatement.setString(1, "TRUE");//flag
                preparedStatement.setString(2, "EXTRACTED");//status
                try (ResultSet resultSet = preparedStatement.executeQuery()) {
                    if (resultSet.isBeforeFirst()) {
                        do {
                            resultSet.next();
                            String idConfig = resultSet.getString("id").trim();
                            FILE_LOCATION = resultSet.getString("folder_name");

                            String recipientEmail = resultSet.getString("error_to_email").trim();
                            if (Files.exists(Paths.get(FILE_LOCATION)) && Files.isDirectory(Paths.get(FILE_LOCATION))) {
                                Connector.updateStatusConfig(configConnection, idConfig, "TRANSFORMING");
                                String hostName = "localhost";
                                String dbName = resultSet.getString("WH_db_name");
                                String username = resultSet.getString("WH_source_username");
                                String password = resultSet.getString("WH_source_password");
                                try (Connection WHConnection = Connector.getConnection(hostName, dbName, username, password)) {


                                    if (Files.exists(Path.of((FILE_LOCATION + "\\transform_data.sql")))) {
                                        String sqlTransform = readFileAsString(FILE_LOCATION + "\\transform_data.sql");
                                        Statement statement = WHConnection.createStatement();
                                        // Chia script thành các câu lệnh riêng biệt
                                        String[] commands = sqlTransform.split(";");
                                        for (String command : commands) {
                                            statement.execute(command);
                                        }
                                        if (Files.exists(Path.of((FILE_LOCATION + "\\check_description_dim.sql")))) {
                                            String check_description_dim = readFileAsString(FILE_LOCATION + "\\check_description_dim.sql");
                                            statement.execute(check_description_dim);
                                            Connector.writeLog(configConnection,
                                                    "UPDATE description_dim TABLE ",
                                                    "Update description_dim table in WH",
                                                    idConfig,
                                                    "WARNING",
                                                    "Description is updated, please check and fix vietnamese name as soon as possible");

                                        } else {
                                            // can't find check_description_dim.sql
                                            Connector.writeLog(configConnection,
                                                    "TRANSFORM",
                                                    "Check description_dim table in WH",
                                                    idConfig,
                                                    "WARNING",
                                                    "Sql file check_description_dim.sql  does not exist or failed to access path");

                                            String subject = "ERROR in Transformation Step";
                                            String body = "Dear Admin,\n\n" +
                                                    "We encountered an error during the transformation step of our process.\n\n" +
                                                    "Error Details: The SQL file 'check_description_dim.sql' does not exist or there was a failure in accessing the specified path.\n\n" +
                                                    "Please take the following actions to resolve the issue:\n" +
                                                    "1. Check if the SQL file 'check_description_dim.sql' exists in the specified location.\n" +
                                                    "2. Ensure that the application has the necessary permissions to access the specified path.\n\n" +
                                                    "If you need further assistance, feel free to contact our support team.\n\n" +
                                                    "Thank you,\nYour Application Team";

                                            SendEmail.sendMail(recipientEmail, subject, body);
                                        }


                                        Connector.updateStatusConfig(configConnection, idConfig, "TRANSFORMED");
                                        Connector.writeLog(configConnection,
                                                "TRANSFORM",
                                                "Transform data",
                                                idConfig,
                                                "SUCCESS",
                                                "");
                                    } else {
                                        // can't find transform_data.sql
                                        Connector.updateFlagConfig(configConnection, idConfig, "FALSE");
                                        Connector.writeLog(configConnection,
                                                "TRANSFORM",
                                                "Transform data",
                                                idConfig,
                                                "ERR",
                                                "Sql file transform_data.sql does not exist or failed to access path");


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

                                    WHConnection.close();

                                } catch (Exception ex) {
                                    //Can't connect to warehouse database
                                    Connector.updateFlagConfig(configConnection, idConfig, "FALSE");
                                    Connector.writeLog(configConnection,
                                            "TRANSFORM",
                                            "Connect to warehouse database",
                                            idConfig,
                                            "ERR",
                                            "Cannot connect to warehouse database");
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


                            } else {
                                // can't find the folder path
                                Connector.updateFlagConfig(configConnection, idConfig, "FALSE");
                                Connector.writeLog(configConnection,
                                        "TRANSFORM",
                                        "Transform data",
                                        idConfig,
                                        "ERR",
                                        "Folder contain sql file does not exist or failed to access path");
                                String subject = "ERROR: Issue Getting Path of SQL Files Folder";
                                String body = "Dear Admin,\n\n" +
                                        "We encountered an error while attempting to retrieve the path of the folder containing SQL files.\n\n" +
                                        "Error Details: The folder containing SQL files does not exist or there was a failure in accessing the specified path.\n\n" +
                                        "Please take the following actions to resolve the issue:\n" +
                                        "1. Check if the folder containing SQL files exists in the specified location.\n" +
                                        "2. Ensure that the application has the necessary permissions to access the specified path.\n\n" +
                                        "If you need further assistance, feel free to contact our support team.\n\n" +
                                        "Thank you,\nYour Application Team";

                                SendEmail.sendMail(recipientEmail, subject, body);

                            }
                        } while (resultSet.next());
                    }
                } catch (Exception ignored) {
                }
            }
            configConnection.close();
        } catch (Exception ignored) {
        }
    }

    private String readFileAsString(String filePath) throws Exception {
        String data ;
        data = new String(Files.readAllBytes(Paths.get(filePath)));
        return data;
    }


    public static void main(String[] args) throws IOException {
        Transform trans = new Transform();
        trans.startTransform();
    }
}
