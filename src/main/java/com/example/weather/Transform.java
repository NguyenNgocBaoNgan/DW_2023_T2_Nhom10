package com.example.weather;

import com.example.weather.DAO.ControlConnector;
import com.example.weather.DAO.WHConnector;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.util.Properties;

public class Transform {
    Properties prop = new Properties();

    {
        try {
            prop.load(ControlConnector.class.getClassLoader().getResourceAsStream("data.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    String FILE_LOCATION;

    public void startTransform() {
        try (Connection configConnection = new ControlConnector().getConnection()) {
            String sqlGetDownloadPath = "SELECT * FROM configuration WHERE  flag = 'TRUE'  AND STATUS = 'EXTRACTED'";
            try (PreparedStatement preparedStatement = configConnection.prepareStatement(sqlGetDownloadPath)) {
                try (ResultSet resultSet = preparedStatement.executeQuery()) {
                    if (resultSet.isBeforeFirst()) {
                        while (resultSet.next()) {

                            String idConfig = resultSet.getString("id").trim();

                            FILE_LOCATION = resultSet.getString("folder_name");
                            if (Files.exists(Paths.get(FILE_LOCATION)) && Files.isDirectory(Paths.get(FILE_LOCATION))) {
                                new ControlConnector().updateStatusConfig(configConnection, idConfig, "TRANSFORMING");
                                //todo cập nhật hostname trong db
                                String hostName = "localhost";
                                String dbName = resultSet.getString("WH_db_name");
                                String username = resultSet.getString("WH_source_username");
                                String password = resultSet.getString("WH_source_password");
                                System.out.println(dbName + " " + username + " " + password);
                                try (Connection WHConnection = new WHConnector(hostName, dbName, username, password).getConnection()) {
                                    try {
                                        String sqlTransform = readFileAsString(FILE_LOCATION+"\\transform.sql");
                                        Statement statement = WHConnection.createStatement();
                                        statement.execute(sqlTransform);

                                        String sqlCheckDim = "SELECT * FROM description_dim WHERE name_vi IS NULL OR name_= ''";
                                        try (PreparedStatement preparedStatement1 = WHConnection.prepareStatement(sqlCheckDim)) {
                                            try (ResultSet des = preparedStatement1.executeQuery()) {

                                                while (des.next()) {
                                                  statement.execute("UPDATE description_dim SET name_vi = name_en WHERE id = " + des.getString(1));
                                                    new ControlConnector().updateStatusConfig(configConnection, idConfig, "TRANSFORMED");
                                                    new ControlConnector().writeLog(configConnection,
                                                            "UPDATE description_dim TABLE ",
                                                            "Update description_dim table in WH",
                                                            idConfig,
                                                            "WARNING",
                                                            "Description is updated, please check and fix vietnamese name as soon as possible");
                                                  //gửi mail
                                                }

                                            }
                                        }
                                        new ControlConnector().updateStatusConfig(configConnection, idConfig, "TRANSFORMED");
                                        new ControlConnector().writeLog(configConnection,
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
                                    new ControlConnector().updateFlagConfig(configConnection, idConfig, "FALSE");
                                    new ControlConnector().writeLog(configConnection,
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
                                new ControlConnector().updateFlagConfig(configConnection, idConfig, "ERR");
                                new ControlConnector().writeLog(configConnection,
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
