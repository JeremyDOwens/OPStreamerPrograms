package tv.opg.streamerprograms;

public class Streamer {
	public final String email;
	public final String name;
	public final String channel;
	public static Streamer[] examples = {
			new Streamer("jeremy@opg.tv", "Jeremy", "tsagh"),
			new Streamer("audrey@opg.tv", "Audrey", "audreyrawr"),
			new Streamer("darklordsen@gmail.com", "D L Sen", "darklordsen"),
			new Streamer("tarabekim@gmail.com", "F8D", "f8_hots"),
			new Streamer("quasiprogramming@gmail.com", "Quincy Pham", "quasipro"),
			new Streamer("twitch.khaljiit@gmail.com", "Julia Rossi", "khaljiit")
	};
	
	public Streamer(String email, String name, String channel) {
		this.email = email;
		this.name = name;
		this.channel = channel;
	}
	
	@Override
	public String toString() {
		return email + "," + channel + "," + name;
	}

}
