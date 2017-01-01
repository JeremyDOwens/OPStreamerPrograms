/**
 * Represents a streamer who participates in a given program.
 * 
 * All FINAL members are declared public to facilitate access via dot notation.
 * 
 * Class ProgramParticipant
 * Bugs: none known
 *
 * @author       Jeremy Owens
 * @company      OP Group
 * @version      1.0
 * @since        2016-12-28
 * @see also     Program, ProgramReward, Streamer, ProgramRule, ProgramEvaluator
 */
package tv.opg.streamerprograms;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.heroku.sdk.jdbc.DatabaseUrl;

public class ProgramParticipant {
	public final Program PROGRAM;
	public final String IGN;
	public final Streamer STREAMER;
	public final String STATUS;
	
	private ProgramParticipant(Program program,  String ign, Streamer streamer, String status) {
		this.PROGRAM = program;
		this.IGN = ign;
		this.STREAMER = streamer;
		this.STATUS = status;

	}
	
	public static ProgramParticipant getParticipant(Program program, Streamer streamer) {
		Connection connection = null;
		ProgramParticipant pp = null;
		try {
	        connection = DatabaseUrl.extract().getConnection();
	        PreparedStatement stmt = connection.prepareStatement("SELECT * FROM ProgramParticipants WHERE program_id = ? AND streamer_id = ? Limit 1");
	        stmt.setInt(1, program.PROGRAM_ID);
	        stmt.setInt(2, streamer.ID);
	        ResultSet rs = stmt.executeQuery();
	        if (rs.next()) {
	        	pp = new ProgramParticipant(program, rs.getString("ign"), streamer, rs.getString("status"));
	        }
	        stmt.close();
		} catch (Exception e){
			System.out.println(e.getMessage());
		} finally {
			if (connection != null) try{connection.close();} catch(SQLException e){}
		}
		return pp;
	} 
	
	public static ProgramParticipant createParticipant(Program program, Streamer streamer, String ign, String status) {
		Connection connection = null;
		try {
	        connection = DatabaseUrl.extract().getConnection();
	        PreparedStatement stmt = connection.prepareStatement("INSERT INTO ProgramParticipants VALUES(?,?,?,?)");
	        stmt.setInt(1, program.PROGRAM_ID);
	        stmt.setInt(2, streamer.ID);
	        stmt.setString(3, ign);
	        stmt.setString(4, status);
	        stmt.executeUpdate();
	        stmt.close();
		} catch (Exception e){
			System.out.println(e.getMessage());
		} finally {
			if (connection != null) try{connection.close();} catch(SQLException e){}
		}
		
		return getParticipant(program, streamer);
	}
	
	public static List<ProgramParticipant> getParticipants(Program program) {
		Connection connection = null;
		List<ProgramParticipant> pp = new ArrayList<>();
		try {
	        connection = DatabaseUrl.extract().getConnection();
	        PreparedStatement ps = connection.prepareStatement("SELECT * FROM ProgramParticipants WHERE program_id = ?");
	        ps.setInt(1, program.PROGRAM_ID);
	        ResultSet rs = ps.executeQuery();
	        while (rs.next()) {
	        	pp.add(new ProgramParticipant(program, rs.getString("ign"), Streamer.getStreamer(rs.getInt("streamer_id")), rs.getString("status")));
	        }
	        rs.close();
	        ps.close();
	        
		} catch (Exception e){
			System.out.println(e.getMessage());
		} finally {
			if (connection != null) try{connection.close();} catch(SQLException e){}
		}
		return pp;
	}
	
	public static ProgramParticipant deleteParticipant(Program program, Streamer streamer) {
		Connection connection = null;
		try {
	        connection = DatabaseUrl.extract().getConnection();
	        PreparedStatement stmt = connection.prepareStatement("DELETE FROM ProgramParticipants WHERE program_id = ? AND streamer_id = ?");
	        stmt.setInt(1, program.PROGRAM_ID);
	        stmt.setInt(2, streamer.ID);
	        stmt.executeUpdate();
	        stmt.close();
		} catch (Exception e){
			System.out.println(e.getMessage());
		} finally {
			if (connection != null) try{connection.close();} catch(SQLException e){}
		}
		
		return getParticipant(program, streamer);
	}
	
	public void updateParticipantIGN(Streamer streamer, Program program, String ign) {
		Connection connection = null;
		try {
	        connection = DatabaseUrl.extract().getConnection();
	        PreparedStatement ps = connection.prepareStatement("UPDATE ProgramParticipants SET ign = ? WHERE streamer_id = ? AND program_id = ?");
	        ps.setString(1, ign);
	        ps.setInt(2, streamer.ID);
	        ps.setInt(3, program.PROGRAM_ID);
	        ps.executeUpdate();
	        ps.close();
	        
		} catch (Exception e){
			System.out.println(e.getMessage());
		} finally {
			if (connection != null) try{connection.close();} catch(SQLException e){}
		}

	}
	public String toString() {
		return PROGRAM + "," + STREAMER + "," + IGN + "," + STATUS;
	}

}
