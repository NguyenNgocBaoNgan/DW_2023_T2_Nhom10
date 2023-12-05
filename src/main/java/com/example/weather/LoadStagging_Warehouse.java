package com.example.weather;
import java.sql.Connection;
import java.sql.*;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;


public class LoadStagging_Warehouse {
	private static final String JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";
    private static final String CONTROL_DB_URL = "jdbc:mysql://localhost:3306/control";
    private static final String STAGING_DB_URL = "jdbc:mysql://localhost:3306/weather_warehouse";
    private static final String CONTROL_DB_USERNAME = "root";
    private static final String CONTROL_DB_PASSWORD = "";
    private static final String STAGING_DB_USERNAME = "root";
    private static final String STAGING_DB_PASSWORD = "";

    private static final String SQL_SELECT_CONFIG_DATA = "SELECT * FROM configuration";
    private static final String SQL_SELECT_RECORDS_STAGING_DATA = "SELECT * FROM records_staging";
    private static final String SQL_SELECT_RECORDS_DATA = "SELECT * FROM records";
    private static final String SQL_UPDATE_CONFIG_STATUS = "UPDATE configuration SET status = ? WHERE flag = ?";
    private static final String SQL_UPDATE_DATA_EXPIRED = "UPDATE records SET is_expired = ? WHERE date_forcast = ? AND time_forcast = ?";
    private static final String SQL_INSERT_DATA = "INSERT INTO records (province_id, time_record, date_record, time_forcast, date_forcast, temperature, feel_like, description_id, wind_direction_id, wind_speed, humidity, uv_index, cloud_cover, precipitation, accumulation) SELECT province, time_record, date_record, time_forcast, date_forcast, temperature, feel_like, description, wind_direction, wind_speed, humidity, uv_index, cloud_cover, precipitation, accumulation FROM records_staging";
    
    private static final String SQL_CHECK_DATA_EXISTS = "UPDATE records AS r1 "
            + "SET is_expired = 'True' "
            + "WHERE (date_forcast, time_forcast) = ( "
            + "  SELECT date_forcast, time_forcast "
            + "  FROM records AS r2 "
            + "  WHERE r1.date_forcast = r2.date_forcast "
            + "    AND r1.time_forcast = r2.time_forcast "
            + "  ORDER BY date_record DESC, time_record DESC "
            + "  LIMIT 1 "
            + ") "
            + "AND (date_record, time_record) < ( "
            + "  SELECT MAX(date_record), MAX(time_record) "
            + "  FROM records "
            + "  WHERE date_forcast = r1.date_forcast "
            + "    AND time_forcast = r1.time_forcast "
            + ")";

    public static void main(String[] args) throws SQLException {
        // Kiểm tra kết nối với control database
        Connection controlConnection = connectToDatabase(CONTROL_DB_URL, CONTROL_DB_USERNAME, CONTROL_DB_PASSWORD);
        if (controlConnection == null) {
            return;
        }

        // Lấy dữ liệu từ bảng config
        ResultSet configData = executeQuery(controlConnection, SQL_SELECT_CONFIG_DATA);
       
        

        // Cập nhật trạng thái của các config đang chạy
        while (configData.next()) {
            int configId = configData.getInt("id");
            String status = configData.getString("status");
            String flag = configData.getString("flag");

    
            
            if (flag.equals("TRUE")&& status.equals("TRANSFORMED")) {
                // Nếu trạng thái là TRANSFORMED, thì cập nhật trạng thái thành LOADING
                updateConfigStatus(controlConnection, configId, "LOADING", flag);

                // Kiểm tra kết nối với staging database
//                System.out.println(STAGING_DB_URL);
//                System.out.println(STAGING_DB_USERNAME);
                System.out.println("bla bla");
                Connection stagingConnection = connectToDatabase(STAGING_DB_URL, STAGING_DB_USERNAME, STAGING_DB_PASSWORD);
                
                
                
//             // Lấy dữ liệu từ bảng stagging
//                ResultSet stagingData = executeQuery(stagingConnection, SQL_SELECT_RECORDS_STAGING_DATA);
//                ResultSet recordData = executeQuery(stagingConnection, SQL_SELECT_RECORDS_DATA);
////                printResultSet(stagingData);
                
                if (stagingConnection == null) {
                    System.out.println("Không thể kết nối với staging database");

                    // Cập nhật Flag=FALSE trong bảng config
                    updateFlagInConfig(controlConnection, configId, "FALSE");

                    // Ghi vào log sự kiện cập nhật flag
                    logEvent(controlConnection, configId, "Update Flag to FALSE", "Connection to Staging DB failed", "ERROR");

                    // Gửi mail thông báo
                    SendEmail.sendMail("Config ID " + configId + " không kết nối với staging database");
                    

                    // Đóng kết nối với control database và kết thúc
                    closeConnection(controlConnection);
                    return;
                }


                //Load dữ liệu từ staging sang record
                transferData(stagingConnection);
                System.out.println("dfdfd");
               //check dữ liệu và gán is_expired = True
                updateIsExpired(stagingConnection);
                
//                int dataCount = getDataCount(stagingConnection, stagingData.getString("date_forcast"), stagingData.getString("time_forcast"));
//                if (dataCount > 0) {
//                    // Nếu dữ liệu đã tồn tại, thì cập nhật cột Is_expired = TRUE
//                    updateDataExpired(stagingConnection, recordData.getString("date_forcast"), recordData.getString("time_forcast"));
//                } else {
//                    // Nếu dữ liệu chưa tồn tại, thì chuyển dữ liệu từ bảng records_staging sang bảng records
//                    insertData(stagingConnection, configData.getString("date_forcast"), configData.getString("time_forcast"), configData.getDouble("temperature"), configData.getDouble("humidity"), configData.getDouble("pressure"));
//                }

                // Đóng kết nối với staging database
                closeConnection(stagingConnection);

                // Cập nhật trạng thái của config thành LOADED
                updateConfigStatus(controlConnection, configId, "LOADED", flag);
            }
        }

        // Đóng kết nối với control database
        closeConnection(controlConnection);
    }

    private static Connection connectToDatabase(String url, String username, String password) throws SQLException {
        try {
            // Đăng ký driver MySQL
            Class.forName("com.mysql.cj.jdbc.Driver");

            // Tạo kết nối
            Connection connection = DriverManager.getConnection(url, username, password);
            connection.setAutoCommit(false);
            return connection;
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            throw new SQLException("MySQL JDBC Driver not found.", e);
        }
    }
    public static void transferData(Connection connection) {
    	 Statement stmt = null;

        try {
            
            // Tạo câu lệnh SQL để chuyển dữ liệu từ bảng 'records' sang 'records_raging'
            String transferQuery = SQL_INSERT_DATA;
            
            // Tạo và thực thi câu lệnh
            stmt = connection.createStatement();
            int rowsAffected = stmt.executeUpdate(transferQuery);
            connection.commit();
            // Hiển thị thông báo sau khi chuyển dữ liệu
            System.out.println(rowsAffected + " rows transferred successfully.");

        } catch (SQLException e) {
            e.printStackTrace();
        } 
        }
    
    public static void updateIsExpired(Connection connection) {
        PreparedStatement preparedStatement = null;

        try {
    //    	String SQL_INSERT_DATA = "INSERT INTO records (province_id, time_record, date_record, time_forcast, date_forcast, temperature, feel_like, description_id, wind_direction_id, wind_speed, humidity, uv_index, cloud_cover, precipitation, accumulation) SELECT province, time_record, date_record, time_forcast, date_forcast, temperature, feel_like, description, wind_direction, wind_speed, humidity, uv_index, cloud_cover, precipitation, accumulation FROM records_staging";
            // Câu lệnh SQL để cập nhật
            String sqlUpdate = SQL_CHECK_DATA_EXISTS;

            // Chuẩn bị câu lệnh
            preparedStatement = connection.prepareStatement(sqlUpdate);

            // Thực thi câu lệnh cập nhật
            int rowsAffected = preparedStatement.executeUpdate();
            connection.commit();
            
            // In thông báo về số dòng bị ảnh hưởng (cập nhật) bởi câu lệnh
            System.out.println("Rows affected: " + rowsAffected);
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            // Đóng tài nguyên (câu lệnh), nhưng không đóng kết nối vì nó được truyền vào từ bên ngoài.
            try {
                if (preparedStatement != null) {
                    preparedStatement.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

//    private static void printResultSet(ResultSet resultSet) throws SQLException {
//        ResultSetMetaData metaData = resultSet.getMetaData();
//        int columnCount = metaData.getColumnCount();
//
//        while (resultSet.next()) {
//            for (int i = 1; i <= columnCount; i++) {
//                Object value = resultSet.getObject(i);
//                System.out.print(value + "\t");
//            }
//            System.out.println();
//        }
//    }
 // Function to execute a query and return the result set
    private static ResultSet executeQuery(Connection connection, String sql) throws SQLException {
        PreparedStatement statement = connection.prepareStatement(sql);
        return statement.executeQuery();
    }

    // Function to update the status of a config
    private static void updateConfigStatus(Connection connection, int configId, String status, String flag) throws SQLException {
        String sql = SQL_UPDATE_CONFIG_STATUS;
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, status);
            statement.setString(2, flag);
            statement.executeUpdate();
            connection.commit();
        }
    }

    // Function to get the count of data for a specific date and hour
    private static int getDataCount(Connection connection, String date, String hour) throws SQLException {
    	
        int count = 0;
        System.out.println(date);
        try (PreparedStatement statement = connection.prepareStatement(SQL_CHECK_DATA_EXISTS)) {
            statement.setString(1, date);
            statement.setString(2, hour);
            ResultSet resultSet = statement.executeQuery();
            
            if (resultSet.next()) {
                count = resultSet.getInt(1);
            }else {
                // Không có hàng nào trong ResultSet
                System.out.println("ResultSet is empty.");
            }
        }
        return count;
    }

    // Function to update data as expired for a specific date and hour
    private static void updateDataExpired(Connection connection, String date, String hour) throws SQLException {
        String sql = SQL_UPDATE_DATA_EXPIRED;
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setBoolean(1, true);
            statement.setString(2, date);
            statement.setString(3, hour);
            statement.executeUpdate();
            connection.commit();
        }
    }

    // Function to insert data into the records table
    private static void insertData(Connection connection, String date, String hour, double temperature, double humidity, double pressure) throws SQLException {
        String sql = SQL_INSERT_DATA;
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, date);
            statement.setString(2, hour);
            statement.setDouble(3, temperature);
            statement.setDouble(4, humidity);
            statement.setDouble(5, pressure);
            statement.executeUpdate();
            connection.commit();
        }
    }
    
 // Function to update Flag in config table
    private static void updateFlagInConfig(Connection connection, int configId, String flag) throws SQLException {
        String sql = "UPDATE config SET flag = ? WHERE id = ?";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, flag);
            statement.setInt(2, configId);
            statement.executeUpdate();
            connection.commit();
        }
    }

    // Function to log an event
    private static void logEvent(Connection connection, int configId, String activityType, String description, String status) {
        String sql = "INSERT INTO log (activity_type, time_stamp, description, config_id, status) VALUES (?, ?, ?, ?, ?)";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, activityType);
            statement.setTimestamp(2, new Timestamp(System.currentTimeMillis()));
            statement.setString(3, description);
            statement.setInt(4, configId);
            statement.setString(5, status);
            statement.executeUpdate();
            connection.commit();
        } catch (SQLException e) {
            e.printStackTrace();
            // Xử lý ngoại lệ nếu cần thiết
        }
    }


    // Function to close a database connection
    private static void closeConnection(Connection connection) throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }

}
