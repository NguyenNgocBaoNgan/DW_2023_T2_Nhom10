package com.example.weather;

import com.example.weather.DAO.Connector;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
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
        Connector connection= new Connector();
        try (Connection configConnection = connection.getControlConnection()) {
            String sqlGetDownloadPath = "SELECT * FROM configuration WHERE  flag = 'TRUE'  AND STATUS = 'EXTRACTED'";
            try (PreparedStatement preparedStatement = configConnection.prepareStatement(sqlGetDownloadPath)) {
                try (ResultSet resultSet = preparedStatement.executeQuery()) {
                    if (resultSet.isBeforeFirst()) {
                        while (resultSet.next()) {

                            String idConfig = resultSet.getString("id").trim();

                            FILE_LOCATION = resultSet.getString("folder_name");
                            if (Files.exists(Paths.get(FILE_LOCATION)) && Files.isDirectory(Paths.get(FILE_LOCATION))) {
                                connection.updateStatusConfig(configConnection, idConfig, "TRANSFORMING");
                                //todo cập nhật hostname trong db
                                String hostName = "localhost";
                                String dbName = resultSet.getString("WH_db_name");
                                String username = resultSet.getString("WH_source_username");
                                String password = resultSet.getString("WH_source_password");
                                System.out.println(dbName + " " + username + " " + password);
                                try (Connection WHConnection = connection.getConnection(hostName, dbName, username, password)) {

                                    try {
                                        String sqlTransform = readFileAsString(FILE_LOCATION+"\\transform.sql");
                                        Statement statement = WHConnection.createStatement();
                                        statement.execute(sqlTransform);

                                        String sqlCheckDim = "SELECT * FROM description_dim WHERE name_vi IS NULL OR name_= ''";
                                        try (PreparedStatement preparedStatement1 = WHConnection.prepareStatement(sqlCheckDim)) {
                                            try (ResultSet des = preparedStatement1.executeQuery()) {

                                                while (des.next()) {
                                                  statement.execute("UPDATE description_dim SET name_vi = name_en WHERE id = " + des.getString(1));
                                                    connection.updateStatusConfig(configConnection, idConfig, "TRANSFORMED");
                                                    connection.writeLog(configConnection,
                                                            "UPDATE description_dim TABLE ",
                                                            "Update description_dim table in WH",
                                                            idConfig,
                                                            "WARNING",
                                                            "Description is updated, please check and fix vietnamese name as soon as possible");
                                                  //gửi mail
                                                }

                                            }
                                        }
                                        connection.updateStatusConfig(configConnection, idConfig, "TRANSFORMED");
                                        connection.writeLog(configConnection,
                                                "TRANSFORM",
                                                "Clean data",
                                                idConfig,
                                                "SUCCESS",
                                                "");
                                    } catch (Exception ex) {
                                        ex.printStackTrace();
                                    }
                                } catch (Exception ex) {
                                    //Can't connect to warehouse database
                                    connection.updateFlagConfig(configConnection, idConfig, "FALSE");
                                    connection.writeLog(configConnection,
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
                                connection.updateFlagConfig(configConnection, idConfig, "ERR");
                                connection.writeLog(configConnection,
                                        "TRANSFORM",
                                        "Clean data",
                                        idConfig,
                                        "ERR",
                                        "Folder contain sql file does not exist or failed to access path");
                                //TODO
                                //Còn 1 bước gửi mail và log
                            }


                        }
                    } else {
                        System.out.println("Không có config hơp lệ");
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static String readFileAsString(String filePath) throws Exception {
        String data = "";
        data = new String(Files.readAllBytes(Paths.get(filePath)));
        return data;
    }


    public static void main(String[] args) throws IOException {
        Transform trans = new Transform();
        trans.startTransform();
    }
}
