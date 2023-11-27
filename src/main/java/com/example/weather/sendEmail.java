package com.example.weather;
import javax.mail.*;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.util.Properties;



public class sendEmail {
	private static final String EMAIL_USERNAME = "your@gmail.com"; 
	private static final String EMAIL_PASSWORD = "123456"; 
	private static final String SMTP_HOST = "smtp.gmail.com"; 
	
	public static void sendMail(String message) {
	    // Cài đặt thông tin người gửi
	    Properties properties = new Properties();
	    properties.put("mail.smtp.host", SMTP_HOST);
	    properties.put("mail.smtp.port", "587");
	    properties.put("mail.smtp.auth", "true");
	    properties.put("mail.smtp.starttls.enable", "true");

	    // Tạo đối tượng Session để xác thực người gửi
	    Session session = Session.getInstance(properties, new Authenticator() {
	        @Override
	        protected PasswordAuthentication getPasswordAuthentication() {
	            return new PasswordAuthentication(EMAIL_USERNAME, EMAIL_PASSWORD);
	        }
	    });

	    try {
	        // Tạo đối tượng Message
	        Message mailMessage = new MimeMessage(session);
	        mailMessage.setFrom(new InternetAddress(EMAIL_USERNAME)); // Địa chỉ người gửi
	        mailMessage.setRecipients(Message.RecipientType.TO, InternetAddress.parse("recipient@example.com")); // Địa chỉ người nhận
	        mailMessage.setSubject("Thông báo từ ứng dụng"); // Tiêu đề email
	        mailMessage.setText(message); // Nội dung email

	        // Gửi email
	        Transport.send(mailMessage);
	        System.out.println("Đã gửi email thành công.");
	    } catch (MessagingException e) {
	        e.printStackTrace();
	        System.out.println("Gửi email thất bại. Lỗi: " + e.getMessage());
	    }
	}

}
