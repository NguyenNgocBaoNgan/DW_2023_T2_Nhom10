package com.example.weather;

import com.example.weather.DAO.Connector;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Locale;

public class Crawler {

    String FILE_LOCATION;
    String province = "";

    public static void main(String[] args) throws IOException, ParseException {
        Crawler crawler = new Crawler();
        crawler.startCrawl();
    }


 
    public void startCrawl() throws IOException, ParseException {
        try (Connection connection = Connector.getControlConnection()) {
 
            String getConfig = readFileAsString("get_config.sql");
            try (PreparedStatement preparedStatement = connection.prepareStatement(getConfig)) {
                preparedStatement.setString(1, "TRUE");//flag
                preparedStatement.setString(2, "PREPARED");//status
                try (ResultSet resultSet = preparedStatement.executeQuery()) {
                    if (resultSet.isBeforeFirst()) {
                        do {
                            resultSet.next();
                            FILE_LOCATION = resultSet.getString("download_path");
                            String idConfig = resultSet.getString("id").trim();
                            String email = resultSet.getString("error_to_email").trim();
                            if (Files.exists(Paths.get(FILE_LOCATION)) && Files.isDirectory(Paths.get(FILE_LOCATION))) {
                                Connector.updateStatusConfig(connection, idConfig, "CRAWLING");
 
                                String sqlGetLinks = readFileAsString("get_link.sql");
                                try (PreparedStatement preparedStatement1 = connection.prepareStatement(sqlGetLinks)) {
                                    try (ResultSet links = preparedStatement1.executeQuery()) {
                                        do {
                                            links.next();
                                            String link = links.getString("link");
                                            String content = crawl(link, new ArrayList<>());
                                            if (content != null) {
                                                exportData(content);
                                            } else {
                                                //Link error
                                                String idLink = links.getString("id");
                                                new Connector().updateFlagDataLinks(connection, idLink, "FALSE");
                                                System.out.println("CRAWL FAILED");

                                                Connector.writeLog(connection,
                                                        "CRAWL",
                                                        "Get data from web",
                                                        idConfig,
                                                        "ERR",
                                                        "Error with link at id is" + idLink);
                                                String subject = "Error: Issue Retrieving Data from Web";
                                                String body = "Dear Admin,\n\n" +
                                                        "We encountered an error while trying to retrieve data from the web in one of our steps. The specific issue is as follows:\n\n" +
                                                        "Error Details: An error occurred with the link identified by ID " + idLink + ".\n\n" +
                                                        "Please review the link associated with ID " + idLink + " to identify and resolve the problem.\n" +
                                                        "If you need further assistance, feel free to contact our support team.\n\n" +
                                                        "Thank you,\nYour Application Team";

                                                SendEmail.sendMail(email, subject, body);

                                            }
                                        } while (links.next());
                                        //Success
                                        Connector.updateStatusConfig(connection, idConfig, "CRAWLED");
                                        Connector.writeLog(connection,
                                                "CRAWL",
                                                "Get data from web",
                                                idConfig,
                                                "SUCCESS",
                                                "");
                                    }
                                }
                            } else {
                                // can't find the download path
                                Connector.updateFlagConfig(connection, idConfig, "FALSE");
                                Connector.writeLog(connection,
                                        "CRAWL",
                                        "Get data from web",
                                        idConfig,
                                        "ERR",
                                        "Download path does not exist or failed to access path");
                                String subject = "Error: Issue Retrieving Data from Web";
                                String body = "Dear Admin,\n\n" +
                                        "We encountered an error while trying to retrieve data from the web in one of our steps. The issue is as follows:\n\n" +
                                        "Error Message: Download path does not exist or failed to access the path.\n\n" +
                                        "Please ensure that the specified download path exists in configuration table.\n" +
                                        "If you need further assistance, feel free to contact our support team.\n\n" +
                                        "Thank you,\nYour Application Team";

                                SendEmail.sendMail(email, subject, body);

                            }
                        } while (resultSet.next());
                    }
                    connection.close();
                }
            }
        } catch (SQLException ignore) {
        }
    }

    public void exportData(String content) throws IOException {
        String fileName = "Crawl_" + province + "_" + getTimeNow() + "_" + getDateNow();
        fileName = fileName.replace(":", "_");
        String file_path = FILE_LOCATION + "\\" + fileName + ".csv";
        System.out.println("File path: " + file_path);
        File file = new File(file_path);
        if (!file.exists()) createFile(file_path);

        Writer writer = new OutputStreamWriter(new FileOutputStream(file_path, true), StandardCharsets.UTF_8);
        writer.write(content + "\n");
        System.out.println("CRAWL SUCCESSED");

    }

    private void createFile(String path) throws FileNotFoundException {
        new OutputStreamWriter(new FileOutputStream(path), StandardCharsets.UTF_8);
        System.out.println("CREATE FILE CSV SUCCESS.");

    }

    private String readFileAsString(String filePath) {
        String data;
        try {
            data = new String(Files.readAllBytes(Paths.get(filePath)));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return data;
    }

    private String crawl(String url, ArrayList<String> visited) {

        Document doc = request(url, visited);
        // Lấy thông tin từ các phần tử HTML cụ thể
        if (doc != null) {
            Element cityElement = doc.selectFirst("h1.header-loc");
            String cityName = cityElement != null ? cityElement.text() : null;
            province = cityName;

            System.out.println(doc.select("div.accordion-item.hour").size() + " " + "doc.select");

            String timeRecord = getTimeNow();
            String dateRecord = getDateNow();

            StringBuilder data = new StringBuilder();
            int i = 0;
            if (doc.select("div.accordion-item.hour").size() >= 24) {
                for (Element hour : doc.select("div.accordion-item.hour")) {
                    if (i == 24) break;

                    if (hour != null) {

                        Element timeForcastElement = hour.select("h2.date div").first();
                        String timeForcast = timeForcastElement != null ? convert24h(timeForcastElement.text().trim()) : "";

                        String dateForcast = getDateNow();


                        Element temperatureElement = hour.select("div.temp.metric").first();
                        String temperature = temperatureElement != null ? getNumber(temperatureElement.text().trim()) : "";

                        Element feelLikeElement = hour.select("div.real-feel__text").first();
                        String feel_like = feelLikeElement != null ? getNumber(feelLikeElement.text().trim()) : "";

                        Element descriptionElement = hour.select("div.phrase").first();
                        String description = descriptionElement != null ? descriptionElement.text().trim() : "";

                        Element windElement = hour.select("p:contains(Wind) span.value").first();
                        String wind = windElement != null ? windElement.text() : "";
                        String windDirection = wind.split(" ")[0].trim();
                        String windSpeed = wind.split(" ")[1].trim();


                        Element humidityElement = hour.select("p:contains(Humidity) span.value").first();
                        String humidity = humidityElement != null ? getNumber(humidityElement.text().trim()) : "0";

                        Element UVindexElement = hour.select("p:contains(Max UV Index) span.value").first();
                        String UVindex = UVindexElement != null ? getNumber(UVindexElement.text()) : "0";

                        Element cloudCoverElement = hour.select("p:contains(Cloud Cover) span.value").first();
                        String cloud_cover = cloudCoverElement != null ? getNumber(cloudCoverElement.text()) : "0";

                        Element precipitationElement = hour.select("div.precip").first();
                        String precipitation = precipitationElement != null ? getNumber(precipitationElement.text()) : "0";

                        Element accumulationElement = hour.select("p:contains(Rain) span.value").first();
                        String accumulation;
                        if (accumulationElement != null) {
                            accumulation = hour.select("p:contains(Rain) span.value").text();
                            accumulation = accumulation.trim();
                            if (accumulation.endsWith("mm")) {
                                accumulation = accumulation.split(" ")[0];
                            } else if (accumulation.endsWith("cm")) {
                                double d = Double.parseDouble(accumulation.split(" ")[0]);
                                double value = d * 10;
                                accumulation = value + "";
                            } else if (accumulation.endsWith("dm")) {
                                double d = Double.parseDouble(accumulation.split(" ")[0]);
                                double value = d * 100;
                                accumulation = value + "";
                            } else if (accumulation.endsWith(" m")) {
                                double d = Double.parseDouble(accumulation.split(" ")[0]);
                                double value = d * 1000;
                                accumulation = value + "";
                            }
                        } else {
                            accumulation = 0 + "";
                        }

                        data.append(cityName).append(";")
                                .append(timeRecord).append(";")
                                .append(dateRecord).append(";")
                                .append(timeForcast).append(";")
                                .append(dateForcast).append(";")
                                .append(temperature).append(";")
                                .append(feel_like).append(";")
                                .append(description).append(";")
                                .append(windDirection).append(";")
                                .append(windSpeed).append(";")
                                .append(humidity).append(";")
                                .append(UVindex).append(";")
                                .append(cloud_cover).append(";")
                                .append(precipitation).append(";")
                                .append(accumulation).append("\n");
                        i++;
                    }

                }
            } else {
                for (Element hour : doc.select("div.accordion-item.hour")) {
                    if (i == 24) break;

                    if (hour != null) {

                        Element timeForcastElement = hour.select("h2.date div").first();
                        String timeForcast = timeForcastElement != null ? convert24h(timeForcastElement.text().trim()) : "";

                        String dateForcast = getDateNow();


                        Element temperatureElement = hour.select("div.temp.metric").first();
                        String temperature = temperatureElement != null ? getNumber(temperatureElement.text().trim()) : "";

                        Element feelLikeElement = hour.select("div.real-feel__text").first();
                        String feel_like = feelLikeElement != null ? getNumber(feelLikeElement.text().trim()) : "";

                        Element descriptionElement = hour.select("div.phrase").first();
                        String description = descriptionElement != null ? descriptionElement.text().trim() : "";

                        Element windElement = hour.select("p:contains(Wind) span.value").first();
                        String wind = windElement != null ? windElement.text() : "";
                        String windDirection = wind.split(" ")[0].trim();
                        String windSpeed = wind.split(" ")[1].trim();


                        Element humidityElement = hour.select("p:contains(Humidity) span.value").first();
                        String humidity = humidityElement != null ? getNumber(humidityElement.text().trim()) : "0";

                        Element UVindexElement = hour.select("p:contains(Max UV Index) span.value").first();
                        String UVindex = UVindexElement != null ? getNumber(UVindexElement.text()) : "0";

                        Element cloudCoverElement = hour.select("p:contains(Cloud Cover) span.value").first();
                        String cloud_cover = cloudCoverElement != null ? getNumber(cloudCoverElement.text()) : "0";

                        Element precipitationElement = hour.select("div.precip").first();
                        String precipitation = precipitationElement != null ? getNumber(precipitationElement.text()) : "0";

                        Element accumulationElement = hour.select("p:contains(Rain) span.value").first();
                        String accumulation;
                        if (accumulationElement != null) {
                            accumulation = hour.select("p:contains(Rain) span.value").text();
                            accumulation = accumulation.trim();
                            if (accumulation.endsWith("mm")) {
                                accumulation = accumulation.split(" ")[0];
                            } else if (accumulation.endsWith("cm")) {
                                double d = Double.parseDouble(accumulation.split(" ")[0]);
                                double value = d * 10;
                                accumulation = value + "";
                            } else if (accumulation.endsWith("dm")) {
                                double d = Double.parseDouble(accumulation.split(" ")[0]);
                                double value = d * 100;
                                accumulation = value + "";
                            } else if (accumulation.endsWith(" m")) {
                                double d = Double.parseDouble(accumulation.split(" ")[0]);
                                double value = d * 1000;
                                accumulation = value + "";
                            }
                        } else {
                            accumulation = 0 + "";
                        }

                        data.append(cityName).append(";")
                                .append(timeRecord).append(";")
                                .append(dateRecord).append(";")
                                .append(timeForcast).append(";")
                                .append(dateForcast).append(";")
                                .append(temperature).append(";")
                                .append(feel_like).append(";")
                                .append(description).append(";")
                                .append(windDirection).append(";")
                                .append(windSpeed).append(";")
                                .append(humidity).append(";")
                                .append(UVindex).append(";")
                                .append(cloud_cover).append(";")
                                .append(precipitation).append(";")
                                .append(accumulation).append("\n");
                        i++;
                    }
                }
                Document doc2 = request(url + "?day=2", visited);
                if (doc2 != null) {
                    for (Element hour : doc2.select("div.accordion-item.hour")) {
                        if (i == 24) break;

                        if (hour != null) {

                            Element timeForcastElement = hour.select("h2.date div").first();
                            String timeForcast = timeForcastElement != null ? convert24h(timeForcastElement.text().trim()) : "";

                            String dateForcast = getNextDate();


                            Element temperatureElement = hour.select("div.temp.metric").first();
                            String temperature = temperatureElement != null ? getNumber(temperatureElement.text().trim()) : "";

                            Element feelLikeElement = hour.select("div.real-feel__text").first();
                            String feel_like = feelLikeElement != null ? getNumber(feelLikeElement.text().trim()) : "";

                            Element descriptionElement = hour.select("div.phrase").first();
                            String description = descriptionElement != null ? descriptionElement.text().trim() : "";

                            Element windElement = hour.select("p:contains(Wind) span.value").first();
                            String wind = windElement != null ? windElement.text() : "";
                            String windDirection = wind.split(" ")[0].trim();
                            String windSpeed = wind.split(" ")[1].trim();


                            Element humidityElement = hour.select("p:contains(Humidity) span.value").first();
                            String humidity = humidityElement != null ? getNumber(humidityElement.text().trim()) : "0";

                            Element UVindexElement = hour.select("p:contains(Max UV Index) span.value").first();
                            String UVindex = UVindexElement != null ? getNumber(UVindexElement.text()) : "0";

                            Element cloudCoverElement = hour.select("p:contains(Cloud Cover) span.value").first();
                            String cloud_cover = cloudCoverElement != null ? getNumber(cloudCoverElement.text()) : "0";

                            Element precipitationElement = hour.select("div.precip").first();
                            String precipitation = precipitationElement != null ? getNumber(precipitationElement.text()) : "0";

                            Element accumulationElement = hour.select("p:contains(Rain) span.value").first();
                            String accumulation;
                            if (accumulationElement != null) {
                                accumulation = hour.select("p:contains(Rain) span.value").text();
                                accumulation = accumulation.trim();
                                if (accumulation.endsWith("mm")) {
                                    accumulation = accumulation.split(" ")[0];
                                } else if (accumulation.endsWith("cm")) {
                                    double d = Double.parseDouble(accumulation.split(" ")[0]);
                                    double value = d * 10;
                                    accumulation = value + "";
                                } else if (accumulation.endsWith("dm")) {
                                    double d = Double.parseDouble(accumulation.split(" ")[0]);
                                    double value = d * 100;
                                    accumulation = value + "";
                                } else if (accumulation.endsWith(" m")) {
                                    double d = Double.parseDouble(accumulation.split(" ")[0]);
                                    double value = d * 1000;
                                    accumulation = value + "";
                                }
                            } else {
                                accumulation = 0 + "";
                            }

                            data.append(cityName).append(";")
                                    .append(timeRecord).append(";")
                                    .append(dateRecord).append(";")
                                    .append(timeForcast).append(";")
                                    .append(dateForcast).append(";")
                                    .append(temperature).append(";")
                                    .append(feel_like).append(";")
                                    .append(description).append(";")
                                    .append(windDirection).append(";")
                                    .append(windSpeed).append(";")
                                    .append(humidity).append(";")
                                    .append(UVindex).append(";")
                                    .append(cloud_cover).append(";")
                                    .append(precipitation).append(";")
                                    .append(accumulation).append("\n");
                            i++;
                        }
                    }
                }

            }


            System.out.println(data);
            return data.toString();
        }
        return null;
    }

    public static String convert24h(String time12h) {
        if (time12h.trim().length() == 4) {
            time12h = "0" + time12h;
        }
        time12h = time12h.substring(0, 2) + ":00 " + time12h.substring(3, 5);
        return LocalTime.parse(time12h, DateTimeFormatter.ofPattern("hh:mm a", Locale.US)).format(DateTimeFormatter.ofPattern("HH:mm"));
    }

    public String getTimeNow() {
        LocalDateTime current = LocalDateTime.now();

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("hh:mm:ss");

        return current.format(formatter);

    }

    public String getDateNow() {
        LocalDateTime current = LocalDateTime.now();

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd-MM-yyyy");

        return current.format(formatter);
    }

    public String getNextDate() {
        LocalDateTime current = LocalDateTime.now();
        LocalDateTime nextDate = current.plusDays(1);
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd-MM-yyyy");
        return nextDate.format(formatter);
    }

    public Document request(String url, ArrayList<String> v) {
        try {
            org.jsoup.Connection con = Jsoup.connect(url);
            Document doc = con.get();

            if (con.response().statusCode() == 200) {
                System.out.println("Link: " + url);
                System.out.println(doc.title());
                v.add(url);

                return doc;
            }
        } catch (Exception e) {
            return null;
        }
        return null;

    }

    public String getNumber(String text) {
        StringBuilder number = new StringBuilder();
        for (char c : text.toCharArray()) {
            if (Character.isDigit(c)) {
                number.append(c);
            }
        }
        if (number.length() > 0) {
            return number.toString();
        } else {
            return null;
        }

    }
}
