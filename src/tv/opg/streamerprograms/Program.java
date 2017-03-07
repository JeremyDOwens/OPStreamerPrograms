/**
 * Represents a project with a system of rules to reward broadcasters for different types of
 * streams on Twitch. Class is represented by objects stored in a database, and can only
 * be instantiated through calling the database. As such, all members are FINAL. The only mutable
 * item is the List of ProgramRule(s), which can only be build through accesing the database.
 * 
 * Assumes production on Heroku with PostgreSQL
 * 
 * All FINAL members are declared public to facilitate access via dot notation.
 * 
 * Class Program
 * Bugs: none known
 *
 * @author       Jeremy Owens
 * @company      OP Group
 * @version      1.0
 * @since        2016-12-28
 * @see also     ProgramParticipant, ProgramReward, Streamer, ProgramRule, ProgramEvaluator
 */
package tv.opg.streamerprograms;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import com.heroku.sdk.jdbc.DatabaseUrl;
import tv.opg.streamerprograms.ProgramRule.*;

public class Program {
	/**ID in database table*/
	public final int PROGRAM_ID;
	/**Program name*/
	public final String PROGRAM_NAME;
	/**Sponsor of the program*/
	public final String SPONSOR;
	/**List of games used in checks*/
	public final String[] GAMES;
	/**List of rules*/
	public final boolean ACTIVE;
	private Map<Integer, ProgramRule> rules;
	
	/**
	 * Standard constructor blocked by throwing an exception.
	 */
	public Program() {
		throw new UnsupportedOperationException("Class cannot be direcly instantiated.");
	}
	
	/**
	 * General constructor, only to be accessed privately.
	 * @param id int
	 * @param programName String
	 * @param sponsor String
	 */
	private Program(int id, String programName, String sponsor, String[] games, boolean active) {
	    this.GAMES = games;
		this.PROGRAM_ID = id;
		this.PROGRAM_NAME = programName;
	    this.SPONSOR = sponsor;
	    this.ACTIVE = active;
	    this.rules = new HashMap<>();

	}
	
	/**
	 * Standard method for getting an instance of a Program. Finds the program in the
	 * database, and returns an object associated with that name.
	 * Usage: Program.getProgram("String with program's name here");
	 * @param programName String the name of the program as stored in the Database
	 * @return Program object for further use.
	 */
	public static Program getProgram(String programName) {	
		Connection connection = null;
		Program program = null;
		try {
	        connection = DatabaseUrl.extract().getConnection(); //Heroku postgres jdbc connection
	        PreparedStatement ps = connection.prepareStatement("SELECT * FROM Programs WHERE programName = ?");
	        ps.setString(1, programName);
	        ResultSet rs = ps.executeQuery();
	        if (rs.next()) {
	        	//Instantiate program with fields from database
	        	program = new Program(rs.getInt("program_id"), rs.getString("programName"), rs.getString("sponsor"), rs.getString("games").split(","), rs.getBoolean("active")); 
	        }
	        rs.close();
	        ps.close();
	        
		} catch (Exception e){
			System.out.println(e.getMessage());
			e.printStackTrace(System.out);
		} finally {
			if (connection != null) try{connection.close();} catch(SQLException e){}
		}
		
		program.setRules(); //Build set of rules.
		return program;
	}
	
	public static Program[] getPrograms() {
		Connection connection = null;
		ArrayList<Program> pList = new ArrayList<>();
		try {
	        connection = DatabaseUrl.extract().getConnection(); //Heroku postgres jdbc connection
	        PreparedStatement ps = connection.prepareStatement("SELECT * FROM Programs;");
	        ResultSet rs = ps.executeQuery();
	        
	        while (rs.next()) {
	        	Program program = new Program(rs.getInt("program_id"), rs.getString("programName"), rs.getString("sponsor"), rs.getString("games").split(","), rs.getBoolean("active"));
	        	program.setRules();
	        	pList.add(program); 
	        }
	        rs.close();
	        ps.close();
	        
		} catch (Exception e){
			System.out.println(e.getMessage());
			e.printStackTrace(System.out);
		} finally {
			if (connection != null) try{connection.close();} catch(SQLException e){}
		}
		Program[] pArr = new Program[pList.size()];
		return pList.toArray(pArr);
	}
	
    public static Program[] getActivePrograms() {
    	Connection connection = null;
		ArrayList<Program> pList = new ArrayList<>();
		try {
	        connection = DatabaseUrl.extract().getConnection(); //Heroku postgres jdbc connection
	        PreparedStatement ps = connection.prepareStatement("SELECT * FROM Programs WHERE active = TRUE;");
	        ResultSet rs = ps.executeQuery();
	        
	        while (rs.next()) {
	        	Program program = new Program(rs.getInt("program_id"), rs.getString("programName"), rs.getString("sponsor"), rs.getString("games").split(","), rs.getBoolean("active"));
	        	program.setRules();
	        	pList.add(program); 
	        }
	        rs.close();
	        ps.close();
	        
		} catch (Exception e){
			System.out.println(e.getMessage());
			e.printStackTrace(System.out);
		} finally {
			if (connection != null) try{connection.close();} catch(SQLException e){}
		}
		Program[] pArr = new Program[pList.size()];
		return pList.toArray(pArr);
	}
	/**
	 * Alternative constructor calling the ID.
	 * @param programID int The program_id stored in the database.
	 * @return Program Object for further usage.
	 */
	public static Program getProgram(int programID) {	
		Connection connection = null;
		Program program = null;
		try {
	        connection = DatabaseUrl.extract().getConnection();
	        PreparedStatement ps = connection.prepareStatement("SELECT * FROM Programs WHERE program_id = ?");
	        ps.setInt(1, programID);
	        ResultSet rs = ps.executeQuery();
	        if (rs.next()) {
	        	program = new Program(rs.getInt("program_id"), rs.getString("programName"), rs.getString("sponsor"), rs.getString("games").split(","), rs.getBoolean("active"));
	        }
	        rs.close();
	        ps.close();
	        
		} catch (Exception e){
			System.out.println(e.getMessage());
			e.printStackTrace(System.out);
		} finally {
			if (connection != null) try{connection.close();} catch(SQLException e){}
		}
		
		program.setRules();
		return program;
	}
	
	/**
	 * Method to create a new program. This method inserts the required information into the Database and creates an
	 * object to represent it.
	 * @param programName String The name of the program.
	 * @param sponsor String The company sponsoring the program.
	 * @return Program An object representing the newly created program.
	 */
	public static Program createProgram(String programName, String sponsor, String games, String programEmail, String emailPW, String rewardFrame) {
		Connection connection = null;
		try {
	        connection = DatabaseUrl.extract().getConnection();
	        PreparedStatement stmt = connection.prepareStatement("INSERT INTO Programs (programName,sponsor,games) VALUES(?,?,?)");
	        stmt.setString(1, programName);
	        stmt.setString(2, sponsor);
	        stmt.setString(3, games);
	        stmt.executeUpdate();
	        stmt.close();
	        Program p = getProgram(programName);
	        PreparedStatement ps = connection.prepareStatement("INSERT INTO ProgramMailInfo VALUES(?,?,?,?)");
	        ps.setInt(1, p.PROGRAM_ID);
	        ps.setString(2, programEmail);
	        ps.setString(3, emailPW);
	        ps.setString(4, rewardFrame);
	        ps.executeUpdate();
	        ps.close();
	    
	        return p;
		} catch (Exception e){
			System.out.println(e.getMessage());
			e.printStackTrace(System.out);
			return getProgram(programName);
		} finally {
			if (connection != null) try{connection.close();} catch(SQLException e){}
		}
	}
	
	/**
	 * Method to get a copy of the list of rules
	 * @return List<ProgramRule> ArrayList of rules
	 */
	public Map<Integer, ProgramRule> getRules() {
		return new HashMap<>(rules);
	}
	
	/**
	 * This method creates a rule for the program. It accepts only arrays for Metric, Operand, and Limit
	 * to allow for the creation of composite rules.
	 * Indeces of arrays should match. So m[0], op[0], and limit[0] would correspond to one portion of the rule.
	 * @param m Metric[] the item being measured
	 * @param op Operand[] greater, less, or equal
	 * @param freq Frequency How often the system operates.
	 * @param limit String[] 
	 * @param reward
	 * @return
	 */
	public Program createRule(Metric[] m, Operand op[], Frequency freq, String[] limit, String reward) {
		if (m.length != op.length || op.length != limit.length) throw new IllegalArgumentException("Arrays must be of the same size.");
		Connection connection = null;
		try {
	        connection = DatabaseUrl.extract().getConnection();
	        PreparedStatement stmt = connection.prepareStatement("INSERT INTO ProgramRules (program_id, metrics, operands, limits, frequency, reward) VALUES(?,?,?,?,?,?)");
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
	
	/**
	 * This method grabs all rules corresponding to the current Program
	 * from the database, compiles them, and adds them to the rules list. 
	 */
	private void setRules() {
		rules = new HashMap<>();
		Connection connection = null;
		try {
	        connection = DatabaseUrl.extract().getConnection();
	        PreparedStatement stmt = connection.prepareStatement("SELECT * FROM ProgramRules WHERE program_id = ?");
	        stmt.setInt(1, this.PROGRAM_ID);
	        ResultSet rs = stmt.executeQuery();
	        while(rs.next()) {
	        	String[] mArr = rs.getString("metrics").split(",");
	        	String[] opArr = rs.getString("operands").split(",");
	        	String[] limArr = rs.getString("limits").split(",");
	        	if (mArr.length != opArr.length || limArr.length != opArr.length) throw new Error("Data anomoly cause arrays to have different lengths.");
	        	int x = mArr.length;
	        	if (x == 1) rules.put(rs.getInt("rule_id"), compileRule(Metric.valueOf(mArr[0]), Operand.valueOf(opArr[0]), Frequency.valueOf(rs.getString("frequency")), limArr[0], rs.getString("reward")));
	        	else {
	        		Metric[] mets = new Metric[x];
	        		Operand[] ops = new Operand[x];
	        		for (int i = 0; i < x; i++) {
	        			mets[i] = Metric.valueOf(mArr[i]);
	        			ops[i] = Operand.valueOf(opArr[i]);
	        		}
	        		rules.put(rs.getInt("rule_id"), compileRule(mets, ops, Frequency.valueOf(rs.getString("frequency")), limArr, rs.getString("reward")));
	        	}
	        }
	        
		} catch (Exception e){
			System.out.println(e.getMessage());
			e.printStackTrace();
		} finally {
			if (connection != null) try{connection.close();} catch(SQLException e){}
		}
	}
	
	/**
	 * Method to compile a composite rule into a ProgramRule.
	 * @param m Metric[] 
	 * @param op Operand[]
	 * @param freq Frequency
	 * @param limit String[]
	 * @param reward String
	 * @return ProgramRule
	 */
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
				return composite.get(composite.size()-1).getMetric();
			}
			public String getReward() {
				return reward;
			}
			public String getLimit() {
				return composite.get(composite.size()-1).getLimit();
			}
			public boolean ruleCheck(Map<Metric, Object> values) {
				for (ProgramRule rule: composite) {
					if (!rule.ruleCheck(values)) return false;
				}
				return true;
			}
		};
	}
	
	/**
	 * Method to return a single boolean rule.
	 * @param m Metric
	 * @param op Operand
	 * @param freq Frequency
	 * @param limit String
	 * @param reward String
	 * @return
	 */
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
					if(Long.class.isAssignableFrom(value.getClass()))
						if (((Long)value).longValue() > Long.parseLong(limit)) return true;
						else return false;
					else throw new IllegalArgumentException("GREATER_THAN can only accept Long.");
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
					if (Long.class.isAssignableFrom(value.getClass()))
						if (((Long)value).longValue() < Long.parseLong(limit)) return true;
						else return false;
					else throw new IllegalArgumentException("LESS_THAN can only accept Long");
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
					if (Long.class.isAssignableFrom(value.getClass()))
						if (((Long)value).longValue() == Long.parseLong(limit)) return true;
						else return false;
					else if (String.class.isAssignableFrom(value.getClass()))
						if (((String)value).equals(limit)) return true;
						else return false;
					else throw new IllegalArgumentException("EQUAL_TO may only accept types Long and String.");
				}
			};
		case ONE_OF :
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
					if (String.class.isAssignableFrom(value.getClass())) {
						String[] limArr = limit.split("_");
						for (String lim: limArr)
						    if (((String)value).equals(lim)) return true;
						return false;
					}
					else throw new IllegalArgumentException("ONE_OF only accepts String.");
				}
			};
		default : throw new IllegalArgumentException("Not a valid Metric.");
		}
	}
	
	/**
	 * A method for deleting a ProgramRule and resetting the rules list.
	 * @param m
	 * @param limit
	 * @param reward
	 * @return
	 */
	public Program deleteRule(int ruleID) {
		Connection connection = null;
		try {
	        connection = DatabaseUrl.extract().getConnection();
	        PreparedStatement ps = connection.prepareStatement("DELETE FROM ProgramRules WHERE rule_id = ?;");
	        ps.setInt(1, ruleID);
	        ps.executeUpdate();
	        ps.close();
	        connection.close();
	        
		} catch (Exception e){
			System.out.println(e.getMessage());
			e.printStackTrace(System.out);
		} finally {
			if (connection != null) try{connection.close();} catch(SQLException e){}
		}
		setRules();

		return this;
	}
	public static Program activate(int programID) {
		Connection connection = null;
		try {
			connection = DatabaseUrl.extract().getConnection();
			PreparedStatement state = connection.prepareStatement("UPDATE Programs SET active = TRUE WHERE program_id = ?");
			state.setInt(1,programID);
			state.executeUpdate();
			state.close();
		}
		catch (Exception e){
			System.out.println(e.getMessage());
			e.printStackTrace(System.out);
		} finally {
			if (connection != null) try{connection.close();} catch(SQLException e){}
		}
		
		
		return getProgram(programID);
	}
	
	public static Program deactivate(int programID) {
		Connection connection = null;
		try {
			connection = DatabaseUrl.extract().getConnection();
			PreparedStatement state = connection.prepareStatement("UPDATE Programs SET active = FALSE WHERE program_id = ?");
			state.setInt(1,programID);
			state.executeUpdate();
			state.close();
		}
		catch (Exception e){
			System.out.println(e.getMessage());
			e.printStackTrace(System.out);
		} finally {
			if (connection != null) try{connection.close();} catch(SQLException e){}
		}
		return getProgram(programID);
	}
	@Override
	public String toString() {
		return this.PROGRAM_NAME + ", " + this.SPONSOR;
	}

}
