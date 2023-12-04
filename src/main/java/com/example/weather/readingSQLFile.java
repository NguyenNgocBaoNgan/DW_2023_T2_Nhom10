package com.example.weather;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
public class readingSQLFile {
    private static final Map<String, String> sqlQueries = readSqlFromFile("path/to/sql/queries.sql");

    public static void main(String[] args) {
        // Sử dụng các câu lệnh SQL từ Map
        String selectConfigDataSql = sqlQueries.get("SQL_SELECT_CONFIG_DATA");
        String updateConfigStatusSql = sqlQueries.get("SQL_UPDATE_CONFIG_STATUS");


    }

    private static Map<String, String> readSqlFromFile(String filePath) {
        Map<String, String> sqlQueries = new HashMap<>();
        try {
            // Đọc tất cả các dòng từ tệp tin SQL
            Files.lines(Paths.get(filePath)).forEach(line -> {
                // Tách tên và nội dung câu lệnh SQL bằng dấu :
                String[] parts = line.split(":");
                if (parts.length == 2) {
                    String queryName = parts[0].trim();
                    String query = parts[1].trim();
                    // Lưu vào Map
                    sqlQueries.put(queryName, query);
                }
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
        return sqlQueries;
    }
}
