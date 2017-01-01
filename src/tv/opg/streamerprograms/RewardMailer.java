package tv.opg.streamerprograms;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import javax.mail.*;
import javax.mail.internet.*;

import com.heroku.sdk.jdbc.DatabaseUrl;

public class RewardMailer {
	private final String rewardFrame;
	private final String from;
	private final String pw;
	private final String acctMgrs;
	
	public RewardMailer(Program program) {
		String ams = System.getenv("ACCT_MGRS");
		String[] amArr = ams.split(",");
		for (String s: amArr) {
			s = s+ "@opg.tv";
		}
		for (int i = 0; i < amArr.length; i++) {
			ams = "";
			if (i < amArr.length-1) ams = ams += amArr[i] + ", ";
			else  ams = ams + amArr[i];
		}
		this.acctMgrs = ams;
		String rf = null;
		String rpw = null;
		String rem = null;
		Connection connection = null;
		try {
			connection = DatabaseUrl.extract().getConnection();
			PreparedStatement statement = connection.prepareStatement("SELECT * FROM ProgramMailInfo WHERE program_id = ? LIMIT 1");
	        statement.setInt(1, program.PROGRAM_ID);
			ResultSet rs = statement.executeQuery();
	        if (rs.next()) {
	        	rem = rs.getString("email");
	        	rpw = System.getenv(rs.getString("emailPW"));
	        	rf = rs.getString("rwFrame");
	        }
	        rs.close();
			statement.close();
	      
		} catch (Exception e){
			System.out.println(e.getMessage());
			e.printStackTrace();

		} finally {
			if (connection != null) try{connection.close();} catch(SQLException e){}
		}
		this.from = rem;
		this.pw = rpw;
		this.rewardFrame = rf;
	}
	
	public void sendAcctMgrMail(String subject, String body) {
		
		 Properties props = new Properties();
	        props.put("mail.smtp.host", "smtp.emailsrvr.com");
	        props.put("mail.smtp.socketFactory.port", "465");
	        props.put("mail.smtp.socketFactory.class",
	            "javax.net.ssl.SSLSocketFactory");
	        props.put("mail.smtp.auth", "true");
	        props.put("mail.smtp.port", "465"); 
	        Session session = Session.getDefaultInstance(props,
	        	    new javax.mail.Authenticator() {
	                            @Override
	                            protected PasswordAuthentication getPasswordAuthentication() {
	                                return new PasswordAuthentication(from, pw);
	                                }
	        });
	        try {
                Message message = new MimeMessage(session);
                message.setFrom(new InternetAddress(from));
                message.setRecipients(Message.RecipientType.TO,
                  InternetAddress.parse(acctMgrs));
            //        InternetAddress.parse("jeremy@opg.tv"));
                message.setRecipients(Message.RecipientType.BCC,
                        InternetAddress.parse("jeremy@opg.tv"));
                message.setSubject(subject);
                message.setText(body);

                Transport.send(message);

            } 
	        catch (MessagingException e) {
                    throw new RuntimeException(e);
            }
	}
	
    public void sendRewardMail(List<ProgramReward> rewards, String subject, String body) {
        Properties props = new Properties();
        props.put("mail.smtp.host", "smtp.emailsrvr.com");
        props.put("mail.smtp.socketFactory.port", "465");
        props.put("mail.smtp.socketFactory.class",
            "javax.net.ssl.SSLSocketFactory");
        props.put("mail.smtp.auth", "true");
        props.put("mail.smtp.port", "465"); 
        Session session = Session.getDefaultInstance(props,
        	    new javax.mail.Authenticator() {
                            @Override
                            protected PasswordAuthentication getPasswordAuthentication() {
                                return new PasswordAuthentication(from, pw);
                                }
        });
        for (ProgramReward reward: rewards) {
            try {
                Message message = new MimeMessage(session);
                message.setFrom(new InternetAddress(from));
                message.setRecipients(Message.RecipientType.TO,
                  InternetAddress.parse(reward.STREAMER.EMAIL));
                message.setRecipients(Message.RecipientType.BCC,
                        InternetAddress.parse("jeremy@opg.tv"));
                message.setSubject(subject);
                message.setText(rewardFrame.replace("OTHER_INFO", body).replace("KEY_CODE", reward.CODE).replace("KEY_DESC", reward.DESCRIPTION));

                Transport.send(message);

                } catch (MessagingException e) {
                    throw new RuntimeException(e);
                    }
            }
        }
   
}