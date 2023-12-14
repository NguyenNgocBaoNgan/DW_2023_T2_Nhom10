package com.example.weather.DAO;

import com.example.weather.Run;
import com.example.weather.SendEmail;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.Properties;

public class Connector {

    private static String username;
    private static String password;

    private static String connectionURL;

    static String currentDir;

    static {
        try {
            currentDir = new File(Run.class.getProtectionDomain().getCodeSource().getLocation().toURI().getPath()).getParent().trim().replace("/", "\\").replace("target", "");
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) throws SQLException, URISyntaxException {
        new Connector().getControlConnection();
    }

    public Connector() throws URISyntaxException {
        readConfig();
    }

    private static void readConfig() {
        try {
            Path path = Paths.get(currentDir + "\\config.txt");
            Properties properties = new Properties();
            properties.load(Files.newBufferedReader(path));
            // Lưu giá trị vào các biến
            String hostName = properties.getProperty("data.hostName");
            String dbName = properties.getProperty("data.dbName");
            username = properties.getProperty("data.username");
            password = properties.getProperty("data.password");
            connectionURL = "jdbc:mysql://" + hostName + "/" + dbName;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static Connection getControlConnection() throws SQLException {
        readConfig();
        //Tạo đối tượng Connection
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(connectionURL, username, password);
            System.out.println("Kết nối control db thành công");
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return conn;
    }

    public static Connection getConnection(String hostName, String dbName, String username, String password) throws SQLException {
        //Tạo đối tượng Connection
        Connection conn = null;
        try {
            conn = DriverManager.getConnection("jdbc:mysql://" + hostName + "/" + dbName, username, password);
            System.out.println("Kết nối " + dbName + " db thành công");
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return conn;
    }


    public static void updateFlagDataLinks(Connection conn, String id, String flag) throws SQLException {
        String updateQuery = readFileAsString(currentDir + "\\updateFlagDataLinks.sql");


        try (PreparedStatement preparedStatement = conn.prepareStatement(updateQuery)) {
            // Thiết lập giá trị tham số cho câu lệnh UPDATE
            preparedStatement.setString(1, flag);
            preparedStatement.setString(2, id);

            // Thực hiện cập nhật
            int rowsAffected = preparedStatement.executeUpdate();

            if (rowsAffected > 0) {
                System.out.println("Cập nhật flag " + flag + " bảng data_link thành công.");
            } else {
                System.out.println("Không có dòng nào trong bảng data_link được cập nhật.");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    public static void updateFlagConfig(Connection conn, String id, String flag) throws SQLException {
        String updateQuery = readFileAsString(currentDir + "\\updateFlagConfig.sql");

        try (PreparedStatement preparedStatement = conn.prepareStatement(updateQuery)) {
            // Thiết lập giá trị tham số cho câu lệnh UPDATE
            preparedStatement.setString(1, flag);
            preparedStatement.setString(2, id);

            // Thực hiện cập nhật
            int rowsAffected = preparedStatement.executeUpdate();

            if (rowsAffected > 0) {
                System.out.println("Cập nhật flag " + flag + " bảng configuration thành công.");
            } else {
                System.out.println("Không có dòng nào trong bảng configuration được cập nhật.");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    public static void updateStatusConfig(Connection conn, String id, String status) throws SQLException {
        String updateQuery = readFileAsString(currentDir + "\\updateStatusConfig.sql");


        try (PreparedStatement preparedStatement = conn.prepareStatement(updateQuery)) {
            // Thiết lập giá trị tham số cho câu lệnh UPDATE
            preparedStatement.setString(1, status);
            preparedStatement.setString(2, id);

            // Thực hiện cập nhật
            int rowsAffected = preparedStatement.executeUpdate();

            if (rowsAffected > 0) {
                System.out.println("Cập nhật trạng thái " + status + " bảng configuration thành công.");
            } else {
                System.out.println("Không có dòng nào trong bảng configuration được cập nhật.");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static String readFileAsString(String filePath) {
        String data;
        try {
            data = new String(Files.readAllBytes(Paths.get(filePath)));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return data;

    }

    // Phương thức trả về đối tượng ResultSet
    public static ResultSet getResultSetWithConfigFlags(Connection configConnection, String flag, String status) throws Exception {
        String selectQuery = readFileAsString(currentDir + "\\get_config.sql");

        PreparedStatement preparedStatement = configConnection.prepareStatement(selectQuery);
        preparedStatement.setString(1, flag);
        preparedStatement.setString(2, status);
        ResultSet result = preparedStatement.executeQuery();
        boolean check = false;
        if (!result.next()) {
            String selectQuery1 = readFileAsString(currentDir + "\\get_config.sql").replace("=", "<>").replace("AND", "OR");
            PreparedStatement preparedStatement1 = configConnection.prepareStatement(selectQuery1);
            preparedStatement1.setString(1, flag);
            preparedStatement1.setString(2, status);
            ResultSet result1 = preparedStatement1.executeQuery();

            while (result1.next()) {
                if (result1.getString("flag").equals("FALSE") ||
                        !(result1.getString("status").trim().endsWith("ED"))) {
                    String recipientEmail = result1.getString("error_to_email").trim();
                    String subject = "General Notification";
                    String body = "Dear Admin,\n\n" +
                            "We have an important notification to share with you:\n\n" +
                            "Notification Details: There is not result " + flag + " and status is " + status + ".\n\n" +
                            "Please review the information and take any necessary actions.\n\n" +
                            "If you need further assistance, feel free to contact our support team.\n\n" +
                            "Thank you,\nYour Application Team";
                    System.out.println("There is a result with flag is "+result1.getString("flag")+" and status is " + result1.getString("status"));
                    SendEmail.sendMail(recipientEmail, subject, body);
                   check = true;
                }
            }

            if (check) System.exit(0);
        }
        // Thực hiện truy vấn và trả về ResultSet
        return preparedStatement.executeQuery();

    }

    public static void writeLog(Connection conn, String activityType, String description, String configId, String status, String errorDetail) throws SQLException {
        String insertQuery = readFileAsString(currentDir + "\\insertLog.sql");

        try (PreparedStatement preparedStatement = conn.prepareStatement(insertQuery)) {
            preparedStatement.setString(1, activityType);
            preparedStatement.setString(2, description);
            preparedStatement.setString(3, configId);
            preparedStatement.setString(4, status);
            preparedStatement.setString(5, errorDetail);

            int rowsInserted = preparedStatement.executeUpdate();
            System.out.println("Inserted log " + activityType + " " + status + " successfully");

        } catch (SQLException e) {
            System.out.println("Failed to insert log");
            e.printStackTrace();
        }
    }

    public static String getCurrentDir() {
        return currentDir;
    }
}
