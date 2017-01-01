/**
 * Rules are added to Programs in order to define their logic. This interface defines the rules
 * through forcing several methods to be implemented.
 * 
 * General use case is as a wrapper for boolean logic to be used in a tv.opg.streamerprogram.Program. Since in Java
 * I can't create a List of function objects, I create a list of objects with a shared method.
 * 
 * When setting the Reward of a rule, note that "SPECIAL:sometexthere" denotes a reward type that needs
 * human handling. Example: SPECIAL:drawingentry would indicate that satisfying the boolean entitles
 * the participants to an entry in a drawing.
 * 
 * When defining a ONE_OF rule, items in the list should be a single string with values separated by an underscore(_).
 * 
 * Interface ProgramRule
 * Bugs: none known
 *
 * @author       Jeremy Owens
 * @company      OP Group
 * @version      1.0
 * @since        2016-12-28
 * @see also     ProgramParticipant, ProgramReward, Streamer, Program, ProgramEvaluator
 */
package tv.opg.streamerprograms;

import java.util.Map;

public interface ProgramRule {
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
		EQUAL_TO,
		ONE_OF
	}
	
	public enum Metric {
		COMPOSITE,
		STATUS,
		STREAMS,
		VIEWERS,
		BCTIME,
		VIEWER_MINUTES,
		FOLLOWER_CHANGE,
		BCTIME_PERCENTAGE,
		VIEWER_PULL,
		VIEWER_MINUTE_PERCENTAGE
	}
	
	public boolean ruleCheck(Map<Metric, Object> values);
	public String getLimit();
	public String getReward();
	public Frequency getFrequency();
	public Metric getMetric();
}
