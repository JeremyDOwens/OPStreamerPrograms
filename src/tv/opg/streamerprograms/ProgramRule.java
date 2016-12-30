/**
 * Rules are added to Programs in order to define their logic. This interface defines the rules
 * through forcing several methods to be implemented.
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
	
	public boolean ruleCheck(Map<Metric, Object> values);
	public String getLimit();
	public String getReward();
	public Frequency getFrequency();
	public Metric getMetric();
}
