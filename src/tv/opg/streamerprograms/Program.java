package tv.opg.streamerprograms;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import com.heroku.sdk.jdbc.DatabaseUrl;

public class Program {
	public enum Frequency {
		WEEKLY,
		MONTHLY,
		WEEK_IN_MONTH,
		YEARLY,
		WEEK_IN_YEAR,
		MONTH_IN_YEAR
	}
	
	public enum Operand {
		GREATER_THAN,
		LESS_THAN,
		EQUAL_TO
	}
	
	public enum Metric {
		COMPOSITE,
		GAME,
		STREAMS,
		VIEWERS,
		BCTIME,
		VIEWER_MINUTES,
		FOLLOWER_CHANGE,
		BCTIME_PERCENTAGE,
		VIEWER_PULL,
		VIEWER_MINUTE_PERCENTAGE
	}

	private interface ProgramRule {
		public boolean ruleCheck(Map<Metric, Object> values);
		public String getLimit();
		public String getReward();
		public Frequency getFrequency();
		public Metric getMetric();
	}
	
	public final int PROGRAM_ID;
	public final String PROGRAM_NAME;
	public final String SPONSOR;
	private List<ProgramRule> rules;
	
	public Program() {
		throw new UnsupportedOperationException("Class cannot be direcly instantiated.");
	}
	
	private Program(int id, String programName, String sponsor) {
	    this.PROGRAM_ID = id;
		this.PROGRAM_NAME = programName;
	    this.SPONSOR = sponsor;
	    this.rules = new ArrayList<>();

	}
	
	public static Program getProgram(String programName) {	
		Connection connection = null;
		Program program = null;
		try {
	        connection = DatabaseUrl.extract().getConnection();
	        PreparedStatement ps = connection.prepareStatement("SELECT * FROM Programs WHERE programName = ?");
	        ps.setString(1, programName);
	        ResultSet rs = ps.executeQuery();
	        if (rs.next()) {
	        	program = new Program(rs.getInt("program_id"), rs.getString("programName"), rs.getString("sponsor"));
	        }
	        rs.close();
	        ps.close();
	        
		} catch (Exception e){
			System.out.println(e.getMessage());
		} finally {
			if (connection != null) try{connection.close();} catch(SQLException e){}
		}
		
		program.setRules();
		return program;
	}
	
	public static Program getProgram(int programID) {	
		Connection connection = null;
		Program program = null;
		try {
	        connection = DatabaseUrl.extract().getConnection();
	        PreparedStatement ps = connection.prepareStatement("SELECT * FROM Programs WHERE program_id = ?");
	        ps.setInt(1, programID);
	        ResultSet rs = ps.executeQuery();
	        if (rs.next()) {
	        	program = new Program(rs.getInt("program_id"), rs.getString("programName"), rs.getString("sponsor"));
	        }
	        rs.close();
	        ps.close();
	        
		} catch (Exception e){
			System.out.println(e.getMessage());
		} finally {
			if (connection != null) try{connection.close();} catch(SQLException e){}
		}
		
		program.setRules();
		return program;
	}
	
	public static Program createProgram(String programName, String sponsor) {
		Connection connection = null;
		try {
	        connection = DatabaseUrl.extract().getConnection();
	        PreparedStatement stmt = connection.prepareStatement("INSERT INTO Programs (programName,sponsor) VALUES(?,?)");
	        stmt.setString(1, programName);
	        stmt.setString(2, sponsor);
	        stmt.executeUpdate();
	        stmt.close();
	    
	        return getProgram(programName);
		} catch (Exception e){
			System.out.println(e.getMessage());
			return getProgram(programName);
		} finally {
			if (connection != null) try{connection.close();} catch(SQLException e){}
		}
	}
	
	public Program createRule(Metric[] m, Operand op[], Frequency freq, String[] limit, String reward) {
		if (m.length != op.length || op.length != limit.length) throw new IllegalArgumentException("Arrays must be of the same size.");
		Connection connection = null;
		try {
	        connection = DatabaseUrl.extract().getConnection();
	        Statement statement = connection.createStatement();
	        statement.execute("CREATE TABLE IF NOT EXISTS ProgramRules(program_id int NOT NULL, metrics varchar(120) NOT NULL, operands varchar(120) NOT NULL, limits varchar(120) NOT NULL, frequency varchar(50) NOT NULL, reward varchar(120) NOT NULL, FOREIGN KEY (program_id) REFERENCES Programs)");
	        statement.close();
	        PreparedStatement stmt = connection.prepareStatement("INSERT INTO ProgramRules VALUES(?,?,?,?,?,?)");
	        stmt.setInt(1, this.PROGRAM_ID);
	        if (m.length > 1) {
	        	StringBuilder builder = new StringBuilder();
		        builder.append(m[0]);
		        for (int i = 1; i < m.length; i++) {
		        	builder.append("," + m[i].toString());
		        }
		        stmt.setString(2, builder.toString());
	        }
	        else stmt.setString(2, m[0].toString());
	        if (op.length > 1) {
	        	StringBuilder builder = new StringBuilder();
		        builder.append(op[0]);
		        for (int i = 1; i < op.length; i++) {
		        	builder.append("," + op[i].toString());
		        }
		        stmt.setString(3, builder.toString());
	        }
	        else stmt.setString(3, op[0].toString());
	        if (limit.length > 1) {
	        	StringBuilder builder = new StringBuilder();
		        builder.append(limit[0]);
		        for (int i = 1; i < limit.length; i++) {
		        	builder.append("," + limit[i].toString());
		        }
		        stmt.setString(4, builder.toString());
	        }
	        else stmt.setString(4, limit[0].toString());
	        stmt.setString(5, freq.toString());
	        stmt.setString(6, reward);
	        stmt.executeUpdate();
	        stmt.close();
	    
	        return getProgram(PROGRAM_NAME);
		} catch (Exception e){
			System.out.println(e.getMessage());
			e.printStackTrace();
			return null;
		} finally {
			if (connection != null) try{connection.close();} catch(SQLException e){}
		}
	}
	
	private void setRules() {
		rules = new ArrayList<>();
		Connection connection = null;
		try {
	        connection = DatabaseUrl.extract().getConnection();
	        Statement statement = connection.createStatement();
	        statement.execute("CREATE TABLE IF NOT EXISTS ProgramRules(program_id int NOT NULL, metrics varchar(120) NOT NULL, operands varchar(120) NOT NULL, limits varchar(120) NOT NULL, frequency varchar(50) NOT NULL, reward varchar(120) NOT NULL, FOREIGN KEY (program_id) REFERENCES Programs)");
	        statement.close();
	        PreparedStatement stmt = connection.prepareStatement("SELECT * FROM ProgramRules WHERE program_id = ?");
	        stmt.setInt(1, this.PROGRAM_ID);
	        ResultSet rs = stmt.executeQuery();
	        while(rs.next()) {
	        	String[] mArr = rs.getString("metrics").split(",");
	        	String[] opArr = rs.getString("operands").split(",");
	        	String[] limArr = rs.getString("limits").split(",");
	        	if (mArr.length != opArr.length || limArr.length != opArr.length) throw new Error("Data anomoly cause arrays to have different lengths.");
	        	int x = mArr.length;
	        	if (x == 1) rules.add(compileRule(Metric.valueOf(mArr[0]), Operand.valueOf(opArr[0]), Frequency.valueOf(rs.getString("frequency")), limArr[0], rs.getString("reward")));
	        	else {
	        		Metric[] mets = new Metric[x];
	        		Operand[] ops = new Operand[x];
	        		for (int i = 0; i < x; i++) {
	        			mets[i] = Metric.valueOf(mArr[i]);
	        			ops[i] = Operand.valueOf(opArr[i]);
	        		}
	        		compileRule(mets, ops, Frequency.valueOf(rs.getString("frequency")), limArr, rs.getString("reward") );
	        	}
	        }
	        
		} catch (Exception e){
			System.out.println(e.getMessage());
			e.printStackTrace();
		} finally {
			if (connection != null) try{connection.close();} catch(SQLException e){}
		}
	}
	
	private ProgramRule compileRule(Metric[] m, Operand[] op, Frequency freq, String[] limit, String reward) {
		if (m.length != op.length || op.length != limit.length) throw new IllegalArgumentException("Arrays must be of the same size.");
		List<ProgramRule> composite = new ArrayList<>();
		for (int i = 0; i < m.length; i++) {
			composite.add(compileRule(m[i],op[i],freq,limit[i],reward));
		}
		return new ProgramRule() {
			public Frequency getFrequency() {
				return freq;
			}
			public Metric getMetric() {
				return Metric.COMPOSITE;
			}
			public String getReward() {
				return reward;
			}
			public String getLimit() {
				return "Composite";
			}
			public boolean ruleCheck(Map<Metric, Object> values) {
				for (ProgramRule rule: composite) {
					if (!rule.ruleCheck(values)) return false;
				}
				return true;
			}
		};
	}
	
	private ProgramRule compileRule(Metric m, Operand op, Frequency freq, String limit, String reward) {
		switch (op) {
		case GREATER_THAN : 
			return new ProgramRule() {
				public Frequency getFrequency() {
					return freq;
				}
				public Metric getMetric() {
					return m;
				}
				public String getReward() {
					return reward;
				}
				public String getLimit() {
					return limit;
				}
				public boolean ruleCheck(Map<Metric, Object> values) {
					Object value = values.get(getMetric());
					if (Long.class.isAssignableFrom(value.getClass())) {
						if (((Long)value).longValue() > Long.parseLong(limit)) return true;
						else return false;
					}
					else throw new IllegalArgumentException("LESS_THAN can only take Long as parameter");
				}
			};
		case LESS_THAN : 
			return new ProgramRule() {
				public Frequency getFrequency() {
					return freq;
				}
				public Metric getMetric() {
					return m;
				}
				public String getReward() {
					return reward;
				}
				public String getLimit() {
					return limit;
				}
				public boolean ruleCheck(Map<Metric, Object> values) {
					Object value = values.get(getMetric());
					if (Long.class.isAssignableFrom(value.getClass())) {
						if (((Long)value).longValue() < Long.parseLong(limit)) return true;
						else return false;
					}
					else throw new IllegalArgumentException("LESS_THAN can only take Long as parameter");
					
				}
			};
		case EQUAL_TO :
			return new ProgramRule() {
				public Frequency getFrequency() {
					return freq;
				}
				public Metric getMetric() {
					return m;
				}
				public String getReward() {
					return reward;
				}
				public String getLimit() {
					return limit;
				}
				public boolean ruleCheck(Map<Metric, Object> values) {
					Object value = values.get(getMetric());
					if (Long.class.isAssignableFrom(value.getClass())) {
						if (((Long)value).longValue() == Long.parseLong(limit)) return true;
						else return false;
					}
					else if (String.class.isAssignableFrom(value.getClass())) {
						if (((String)value).equals(limit)) return true;
						else return false;
					}
					else throw new IllegalArgumentException("EQUAL_TO can only take Long or String as parameter");
				}
			};
		default : throw new IllegalArgumentException("Not a valid Metric.");
		}
	}
	
	public Program deleteRule(String m, String limit, String reward) {
		Connection connection = null;
		try {
	        connection = DatabaseUrl.extract().getConnection();
	        PreparedStatement ps = connection.prepareStatement("DELETE FROM ProgramRules WHERE program_id = ? AND limits = ? AND metrics = m AND reward = ?");
	        ps.setInt(1, this.PROGRAM_ID);
	        ps.setString(2, limit);
	        ps.setString(3, m);
	        ps.setString(4, reward);
	        ps.executeUpdate();
	        ps.close();
	        
		} catch (Exception e){
			System.out.println(e.getMessage());
		} finally {
			if (connection != null) try{connection.close();} catch(SQLException e){}
		}
		setRules();

		return this;
	}
	
	@Override
	public String toString() {
		return this.PROGRAM_NAME + ", " + this.SPONSOR;
	}

}
