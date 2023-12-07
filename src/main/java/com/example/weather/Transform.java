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
    Properties prop = new Properties();

    {
        try {
            prop.load(Connector.class.getClassLoader().getResourceAsStream("data.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    String FILE_LOCATION;

    public void startTransform() {
        Connector connection = new Connector();
        try (Connection configConnection = connection.getControlConnection()) {
            String getConfig = Connector.readFileAsString("get_config.sql");
            try (PreparedStatement preparedStatement = configConnection.prepareStatement(getConfig)) {
                preparedStatement.setString(1, "TRUE");//flag
                preparedStatement.setString(2, "EXTRACTED");//status
                try (ResultSet resultSet = preparedStatement.executeQuery()) {
                    if (resultSet.isBeforeFirst()) {
                        do {
                            resultSet.next();
                            String idConfig = resultSet.getString("id").trim();
                            FILE_LOCATION = resultSet.getString("folder_name");

                            System.out.println(resultSet);
                            if (Files.exists(Paths.get(FILE_LOCATION)) && Files.isDirectory(Paths.get(FILE_LOCATION))) {
                                connection.updateStatusConfig(configConnection, idConfig, "TRANSFORMING");
                                //todo cập nhật hostname trong db
                                String hostName = "localhost";
                                String dbName = resultSet.getString("WH_db_name");
                                String username = resultSet.getString("WH_source_username");
                                String password = resultSet.getString("WH_source_password");
                                System.out.println(dbName + " " + username + " " + password);
                                try (Connection WHConnection = Connector.getConnection(hostName, dbName, username, password)) {


                                        if (Files.exists(Path.of((FILE_LOCATION + "\\transform_data.sql")))) {
                                            String sqlTransform = Connector.readFileAsString(FILE_LOCATION + "\\transform_data.sql");
                                            Statement statement = WHConnection.createStatement();
                                            // Chia script thành các câu lệnh riêng biệt
                                            String[] commands = sqlTransform.split(";");
                                            for (String command : commands) {
                                               statement.execute(command);
                                            }
                                            if (Files.exists(Path.of((FILE_LOCATION + "\\check_description_dim.sql")))) {
                                                String check_description_dim = Connector.readFileAsString(FILE_LOCATION + "\\check_description_dim.sql");
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
                                                //TODO
                                                //Còn 1 bước gửi mail và log
                                            }


                                            connection.updateStatusConfig(configConnection, idConfig, "TRANSFORMED");
                                            Connector.writeLog(configConnection,
                                                    "TRANSFORM",
                                                    "Clean data",
                                                    idConfig,
                                                    "SUCCESS",
                                                    "");
                                        } else {
                                            // can't find transform_data.sql
                                            Connector.updateFlagConfig(configConnection, idConfig, "FALSE");
                                            Connector.writeLog(configConnection,
                                                    "TRANSFORM",
                                                    "Clean data",
                                                    idConfig,
                                                    "ERR",
                                                    "Sql file transform_data.sql does not exist or failed to access path");
                                            //TODO
                                            //Còn 1 bước gửi mail và log
                                        }

                                        WHConnection.close();

                                } catch (Exception ex) {
                                    //Can't connect to warehouse database
                                    Connector.updateFlagConfig(configConnection, idConfig, "FALSE");
                                    Connector.writeLog(configConnection,
                                            "TRANSFORM",
                                            "Clean data",
                                            idConfig,
                                            "ERR",
                                            "Cannot connect to warehouse database");
                                    //TODO
                                    //Còn 1 bước gửi mail`
                                }


                            } else {
                                // can't find the folder path
                                Connector.updateFlagConfig(configConnection, idConfig, "FALSE");
                                Connector.writeLog(configConnection,
                                        "TRANSFORM",
                                        "Clean data",
                                        idConfig,
                                        "ERR",
                                        "Folder contain sql file does not exist or failed to access path");
                                //TODO
                                //Còn 1 bước gửi mail và log
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




    public static void main(String[] args) throws IOException {
        Transform trans = new Transform();
        trans.startTransform();
    }
}
