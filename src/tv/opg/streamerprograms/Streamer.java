/**
 * Class to represent a streamer who may take part in any number of programs. Exists as an
 * abstraction of a database entity.
 * 
 * Class Streamer
 * Bugs: none known
 *
 * @author       Jeremy Owens
 * @company      OP Group
 * @version      1.0
 * @since        2016-12-28
 * @see also     ProgramParticipant, ProgramReward, ProgramRule, Program, ProgramEvaluator
 */
package tv.opg.streamerprograms;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import com.heroku.sdk.jdbc.DatabaseUrl;

public class Streamer {
	public final int ID;
	public final String EMAIL;
	public final String NAME;
	public final String CHANNEL;

	
	private Streamer(int id, String email, String name, String channel) {
		this.ID = id;
		this.EMAIL = email;
		this.NAME = name;
		this.CHANNEL = channel;
	}
	public static Streamer getStreamer(int streamerID) {
		Connection connection = null;
		Streamer streamer = null;
		try {
	        connection = DatabaseUrl.extract().getConnection();
	        PreparedStatement ps = connection.prepareStatement("SELECT * FROM Streamers WHERE streamer_id = ?");
	        ps.setInt(1, streamerID);
	        ResultSet rs = ps.executeQuery();
	        if (rs.next()) {
	        	streamer = new Streamer(rs.getInt("streamer_id"), rs.getString("email"), rs.getString("name"), rs.getString("channel"));
	        }
	        rs.close();
	        ps.close();
	        
		} catch (Exception e){
			System.out.println(e.getMessage());
		} finally {
			if (connection != null) try{connection.close();} catch(SQLException e){}
		}
		return streamer;
	}
	
	public static Streamer getStreamer(String emailOrChannel) {
		emailOrChannel = emailOrChannel.toLowerCase();
		Connection connection = null;
		Streamer streamer = null;
		try {
	        connection = DatabaseUrl.extract().getConnection();
	        PreparedStatement ps = null;
	        if (emailOrChannel.contains("@"))
	            ps = connection.prepareStatement("SELECT * FROM Streamers WHERE email = ?");
	        else
	        	ps = connection.prepareStatement("SELECT * FROM Streamers WHERE channel = ?");
	        ps.setString(1, emailOrChannel);
	        ResultSet rs = ps.executeQuery();
	        if (rs.next()) {
	        	streamer = new Streamer(rs.getInt("streamer_id"), rs.getString("email"), rs.getString("name"), rs.getString("channel"));
	        }
	        rs.close();
	        ps.close();
	        
		} catch (Exception e){
			System.out.println(e.getMessage());
		} finally {
			if (connection != null) try{connection.close();} catch(SQLException e){}
		}
		return streamer;
	}
	
	public static Streamer createStreamer(String email, String name, String channel) {
		Connection connection = null;
		try {
	        connection = DatabaseUrl.extract().getConnection();
	        PreparedStatement stmt = connection.prepareStatement("INSERT INTO Streamers (email,name,channel) VALUES(?,?,?)");
	        stmt.setString(1, email);
	        stmt.setString(2, name);
	        stmt.setString(3, channel);
	        stmt.executeUpdate();
	        stmt.close();

		} catch (Exception e){
			System.out.println(e.getMessage());
		} finally {
			if (connection != null) try{connection.close();} catch(SQLException e){}
		}
		return getStreamer(email);
	}
	
	public Streamer updateEmail(String email) {
		Connection connection = null;
		try {
	        connection = DatabaseUrl.extract().getConnection();
	        PreparedStatement ps = connection.prepareStatement("UPDATE Streamers SET email = ? WHERE streamer_id = ?");
	        ps.setString(1, email);
	        ps.setInt(2, this.ID);
	        ps.executeUpdate();
	        ps.close();
	        
		} catch (Exception e){
			System.out.println(e.getMessage());
		} finally {
			if (connection != null) try{connection.close();} catch(SQLException e){}
		}
		return getStreamer(this.ID);
	}
	
	public Streamer updateChannel(String channel) {
		Connection connection = null;
		channel=channel.toLowerCase();
		try {
	        connection = DatabaseUrl.extract().getConnection();
	        PreparedStatement ps = connection.prepareStatement("UPDATE Streamers SET channel = ? WHERE streamer_id = ?");
	        ps.setString(1, channel);
	        ps.setInt(2, this.ID);
	        ps.executeUpdate();
	        ps.close();
	        
		} catch (Exception e){
			System.out.println(e.getMessage());
		} finally {
			if (connection != null) try{connection.close();} catch(SQLException e){}
		}
		return getStreamer(this.ID);
	}
	
	public Streamer updateName(String name) {
		throw new UnsupportedOperationException("Cannot change streamer name at this point.");
	}
	
	@Override
	public String toString() {
		return EMAIL + "," + CHANNEL + "," + NAME;
	}
	
	
		

}
