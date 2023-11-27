package com.example.weather.DAO;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class WHConnector {
    private String hostName;
    private String dbName;
    private String username ;
    private String password;

    private String connectionURL = "jdbc:mysql://" + hostName + "/" + dbName;

    public Connection getConnection() throws SQLException {
        //Tạo đối tượng Connection
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(connectionURL, username, password);
            System.out.println("Kết nối warehouse db thành công");
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return conn;
    }

    public WHConnector(String hostName, String dbName, String username, String password) {
        this.hostName = hostName;
        this.dbName = dbName;
        this.username = username;
        this.password = password;
    }

    public static void main(String[] args) throws SQLException {
    }
}
