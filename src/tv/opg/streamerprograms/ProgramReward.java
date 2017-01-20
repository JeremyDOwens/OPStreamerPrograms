/**
 * Class to represent a program reward. Exists as an
 * abstraction of a database entity.
 * 
 * Class ProgramReward
 * Bugs: none known
 *
 * @author       Jeremy Owens
 * @company      OP Group
 * @version      1.0
 * @since        2016-12-28
 * @see also     ProgramParticipant, ProgramRule, Streamer, Program, ProgramEvaluator
 */
package tv.opg.streamerprograms;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import com.heroku.sdk.jdbc.DatabaseUrl;

public class ProgramReward {
	public final String CODE;
	public final Program PROGRAM;
	public final String DESCRIPTION;
	public final Streamer STREAMER;
	public final Timestamp DATEAWARDED;
	public final Boolean ISASSIGNED;
	public final Boolean WASSENT;
	
	
	private ProgramReward(Program program, String code, String description, Streamer streamer, Timestamp dateAwarded, Boolean isAssigned, Boolean wasSent){
		this.PROGRAM = program;
		this.CODE = code;
		this.DESCRIPTION = description;
		this.STREAMER = streamer;
		this.DATEAWARDED = dateAwarded;
		this.ISASSIGNED = isAssigned;
		this.WASSENT = wasSent;
	}
	
	public static ProgramReward getReward(String code) {
		Connection connection = null;
		ProgramReward pr = null;
		try {
	        connection = DatabaseUrl.extract().getConnection();
	        PreparedStatement ps = connection.prepareStatement("SELECT * FROM ProgramRewards WHERE code = ? LIMIT 1");
	        ps.setString(1, code);
	        ResultSet rs = ps.executeQuery();
	        if (rs.next()) {
	        	pr = new ProgramReward(
	        			Program.getProgram(rs.getInt("program_id")), 
	        			rs.getString("code"), 
	        			rs.getString("description"), 
	        			rs.getObject("streamer_id") != null ? Streamer.getStreamer(rs.getInt("streamer_id")) : null, 
	        			rs.getTimestamp("dateAwarded"), 
	        			rs.getBoolean("isAssigned"), 
	        			rs.getBoolean("wasSent"));
	        }
	        rs.close();
	        ps.close();
	        
		} catch (Exception e){
			System.out.println(e.getMessage());
			e.printStackTrace(System.out);
		} finally {
			if (connection != null) try{connection.close();} catch(SQLException e){}
		}
		return pr;
	}
	
	public static ProgramReward createReward(Program program, String code, String description) {
		Connection connection = null;
		try {
	        connection = DatabaseUrl.extract().getConnection();
	        PreparedStatement stmt = connection.prepareStatement("INSERT INTO ProgramRewards VALUES(?,?,?,?,?,?,?)");
        	stmt.setString(1, code);
        	stmt.setInt(2, program.PROGRAM_ID);
        	stmt.setString(3, description); 	
        	stmt.setNull(4, java.sql.Types.INTEGER);
        	stmt.setTimestamp(5, new Timestamp(0));
        	stmt.setBoolean(6, false);
        	stmt.setBoolean(7, false);
	        stmt.executeUpdate();
	        stmt.close();
		} catch (Exception e){
			System.out.println(e.getMessage());
			e.printStackTrace(System.out);
		} finally {
			if (connection != null) try{connection.close();} catch(SQLException e){}
		}
		return getReward(code);
	}
	
	public static List<ProgramReward> getRewards(Program program) {
		Connection connection = null;
		List<ProgramReward> pr = new ArrayList<>();
		try {
	        connection = DatabaseUrl.extract().getConnection();
	        PreparedStatement ps = connection.prepareStatement("SELECT * FROM ProgramRewards WHERE program_id = ?");
	        ps.setInt(1, program.PROGRAM_ID);
	        ResultSet rs = ps.executeQuery();
	        while (rs.next()) {
	        	pr.add(new ProgramReward(
	        			program, 
	        			rs.getString("code"), 
	        			rs.getString("description"), 
	        			rs.getObject("streamer_id") != null ? Streamer.getStreamer(rs.getInt("streamer_id")) : null, 
	        			rs.getTimestamp("dateAwarded"), 
	        			rs.getBoolean("isAssigned"), 
	        			rs.getBoolean("wasSent")));
	        }
	        rs.close();
	        ps.close();
	        
		} catch (Exception e){
			System.out.println(e.getMessage());
			e.printStackTrace(System.out);
		} finally {
			if (connection != null) try{connection.close();} catch(SQLException e){}
		}
		return pr;
	}
	
	public static List<ProgramReward> getRewards(Streamer streamer) {
		Connection connection = null;
		List<ProgramReward> pr = new ArrayList<>();
		try {
	        connection = DatabaseUrl.extract().getConnection();
	        PreparedStatement ps = connection.prepareStatement("SELECT * FROM ProgramRewards WHERE program_id = ?");
	        ps.setInt(1, streamer.ID);
	        ResultSet rs = ps.executeQuery();
	        while (rs.next()) {
	        	pr.add(new ProgramReward(
	        			Program.getProgram(rs.getInt("program_id")), 
	        			rs.getString("code"), 
	        			rs.getString("description"), 
	        			streamer, 
	        			rs.getTimestamp("dateAwarded"), rs.getBoolean("isAssigned"), 
	        			rs.getBoolean("wasSent")));
	        }
	        rs.close();
	        ps.close();
	        
		} catch (Exception e){
			System.out.println(e.getMessage());
			e.printStackTrace(System.out);
		} finally {
			if (connection != null) try{connection.close();} catch(SQLException e){}
		}
		return pr;
	}
	
	public static List<ProgramReward> getUnassignedRewards(Program program, String type) {
		Connection connection = null;
		List<ProgramReward> pr = new ArrayList<>();
		try {
	        connection = DatabaseUrl.extract().getConnection();
	        PreparedStatement ps = connection.prepareStatement("SELECT * FROM ProgramRewards WHERE program_id = ? AND isAssigned = false AND description = ?");
	        ps.setInt(1, program.PROGRAM_ID);
	        ps.setString(2, type);
	        ResultSet rs = ps.executeQuery();
	        while (rs.next()) {
	        	pr.add(new ProgramReward(
	        			program, 
	        			rs.getString("code"), 
	        			rs.getString("description"), 
	        			null, 
	        			rs.getTimestamp("dateAwarded"), 
	        			rs.getBoolean("isAssigned"),
	        			rs.getBoolean("wasSent")));
	        }
	        rs.close();
	        ps.close();
	        
		} catch (Exception e){
			System.out.println(e.getMessage());
			e.printStackTrace(System.out);
		} finally {
			if (connection != null) try{connection.close();} catch(SQLException e){}
		}
		return pr;
	}
	
	public static ProgramReward getOneUnassignedReward(Program program, String type) {
		Connection connection = null;
		ProgramReward pr = null;
		try {
	        connection = DatabaseUrl.extract().getConnection();
	        PreparedStatement ps = connection.prepareStatement("SELECT * FROM ProgramRewards WHERE program_id = ? AND isAssigned = false AND description = ? LIMIT 1");
	        ps.setInt(1, program.PROGRAM_ID);
	        ps.setString(2, type);
	        ResultSet rs = ps.executeQuery();
	        if (rs.next()) {
	        	pr = new ProgramReward(
	        			program, 
	        			rs.getString("code"), 
	        			rs.getString("description"), 
	        			null, 
	        			rs.getTimestamp("dateAwarded"), 
	        			rs.getBoolean("isAssigned"), 
	        			rs.getBoolean("wasSent"));
	        }
	        rs.close();
	        ps.close();
	        
		} catch (Exception e){
			System.out.println(e.getMessage());
			e.printStackTrace(System.out);
		} finally {
			if (connection != null) try{connection.close();} catch(SQLException e){}
		}
		return pr;
	}
	
	public static List<ProgramReward> getUnsentRewards(Program program) {
		Connection connection = null;
		List<ProgramReward> pr = new ArrayList<>();
		try {
	        connection = DatabaseUrl.extract().getConnection();
	        PreparedStatement ps = connection.prepareStatement("SELECT * FROM ProgramRewards WHERE program_id = ? AND isAssigned = true AND wasSent = false ORDER BY streamer_id");
	        ps.setInt(1, program.PROGRAM_ID);
	        ResultSet rs = ps.executeQuery();
	        while (rs.next()) {
	        	pr.add(new ProgramReward(program, rs.getString("code"), rs.getString("description"), Streamer.getStreamer(rs.getInt("streamer_id")), rs.getTimestamp("dateAwarded"), rs.getBoolean("isAssigned"), rs.getBoolean("wasSent")));
	        }
	        rs.close();
	        ps.close();
	        
		} catch (Exception e){
			System.out.println(e.getMessage());
			e.printStackTrace(System.out);
		} finally {
			if (connection != null) try{connection.close();} catch(SQLException e){}
		}
		return pr;
	}
	
	public static List<String> findRewardTypes(Program program) {
		Connection connection = null;
		List<String> list = new ArrayList<>();
		try {
	        connection = DatabaseUrl.extract().getConnection();
	        PreparedStatement ps = connection.prepareStatement("SELECT DISTINCT description FROM ProgramRewards WHERE program_id = ?");
	        ps.setInt(1, program.PROGRAM_ID);
	        ResultSet rs = ps.executeQuery();
	        while (rs.next()) {
	        	list.add(rs.getString("description"));
	        }
	        rs.close();
	        ps.close();
	        
		} catch (Exception e){
			System.out.println(e.getMessage());
			e.printStackTrace(System.out);
		} finally {
			if (connection != null) try{connection.close();} catch(SQLException e){}
		}
		return list;
	}
	
	public ProgramReward assignReward(int streamer_id, Timestamp time) {
		Connection connection = null;
		try {
	        connection = DatabaseUrl.extract().getConnection();
	        PreparedStatement ps = connection.prepareStatement("UPDATE ProgramRewards SET isAssigned = CAST(1 as boolean), streamer_id = ?, dateAwarded = ? WHERE code = ?");
	        ps.setInt(1, streamer_id);
	        ps.setTimestamp(2, time);
	        ps.setString(3, CODE);
	        ps.executeUpdate();
	        ps.close();
	        
		} catch (Exception e){
			System.out.println(e.getMessage());
			e.printStackTrace(System.out);
		} finally {
			if (connection != null) try{connection.close();} catch(SQLException e){}
		}
		return getReward(this.CODE);
	}
	
	public void sendRewards(List<ProgramReward> rewards, String subject, String body) {
		Connection connection = null;
		RewardMailer mailer = new RewardMailer(PROGRAM);
		mailer.sendRewardMail(rewards, subject, body);
		try {
	        connection = DatabaseUrl.extract().getConnection();        
	        String psBuild = "UPDATE ProgramRewards SET wasSent = CAST(1 as boolean) WHERE code = ?";
	        for (int i = 1;i < rewards.size();i++) {
	        	psBuild = psBuild + " OR code = ?";
	        }
	        PreparedStatement ps = connection.prepareStatement(psBuild);
	        for (int i = 0; i < rewards.size(); i++) {
	        	ps.setString(i+1, rewards.get(i).CODE);
	        }
	        ps.executeUpdate();
	        ps.close();
	        
		} catch (Exception e){
			System.out.println(e.getMessage());
			e.printStackTrace(System.out);
		} finally {
			if (connection != null) try{connection.close();} catch(SQLException e){}
		}
	}
	
	public static ProgramReward unassignReward(String code) {
		Connection connection = null;
		try {
	        connection = DatabaseUrl.extract().getConnection();
	        PreparedStatement ps = connection.prepareStatement("UPDATE ProgramRewards SET isAssigned = CAST(0 as boolean), streamer_id = ?, dateAwarded = ? WHERE code = ?");
	        ps.setNull(1, java.sql.Types.INTEGER);
	        ps.setTimestamp(2, new Timestamp(0));
	        ps.setString(3, code);
	        ps.executeUpdate();
	        ps.close();
	        
		} catch (Exception e){
			System.out.println(e.getMessage());
			e.printStackTrace(System.out);
		} finally {
			if (connection != null) try{connection.close();} catch(SQLException e){}
		}
		return getReward(code);
	}
	
	public String toString() {
		return CODE + "," + DESCRIPTION + "," + STREAMER.CHANNEL;
	}

}
