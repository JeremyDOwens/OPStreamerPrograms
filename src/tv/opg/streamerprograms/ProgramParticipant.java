package tv.opg.streamerprograms;

public class ProgramParticipant {
	public final Program program;
	public final String game;
	public final String ign;
	public final Streamer streamer;
	public final String status;
	
	public ProgramParticipant(Program program, String game,  String ign, Streamer streamer, String status) {
		this.program = program;
		this.game = game;
		this.ign = ign;
		this.streamer = streamer;
		this.status = status;

	}
	
	public String toString() {
		return program + "," + streamer + "," + game + "," + ign + "," + status;
	}

}
