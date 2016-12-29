package tv.opg.streamerprograms;

import java.sql.Timestamp;

public class ProgramReward {
	public final String code;
	public final Program program;
	public final String description;
	public final Streamer streamer;
	public final Timestamp dateAwarded;
	public final Boolean isAssigned;
	public final Boolean wasSent;
	
	
	public ProgramReward(Program program, String code, String description, Streamer streamer, Timestamp dateAwarded, Boolean isAssigned, Boolean wasSent){
		this.program = program;
		this.code = code;
		this.description = description;
		this.streamer = streamer;
		this.dateAwarded = dateAwarded;
		this.isAssigned = isAssigned;
		this.wasSent = wasSent;
	}
	

	
	public String toString() {
		return code + "," + description + "," + streamer.channel;
	}

}
