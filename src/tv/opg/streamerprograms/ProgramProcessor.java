package tv.opg.streamerprograms;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import com.heroku.sdk.jdbc.DatabaseUrl;
import tv.opg.streamerprograms.ProgramRule.Frequency;
import tv.opg.streamerprograms.ProgramRule.Metric;
import tv.opg.twitchdata.Broadcast;
import tv.opg.twitchdata.StreamSnapshot;

public final class ProgramProcessor {
	Program program;
	
	public ProgramProcessor(Program program) {
		this.program = program;
	}
	
	public void processLastWeek() {
		List<ProgramRule> rules = new ArrayList<>();
		program.getRules().forEach((k,r) -> {
			if (r.getFrequency() == Frequency.WEEKLY) rules.add(r);
		});
		TimeZone tz = TimeZone.getTimeZone("America/Los_Angeles");
		Calendar cal = Calendar.getInstance(tz);
		cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MILLISECOND, 0);
		int offset = cal.get(Calendar.DAY_OF_WEEK)-Calendar.MONDAY;
		cal.add(Calendar.DAY_OF_YEAR, -offset);
		Timestamp finish = new Timestamp(cal.getTimeInMillis());
		cal.add(Calendar.DAY_OF_YEAR, -7);
		Timestamp start = new Timestamp(cal.getTimeInMillis());
		cal.add(Calendar.DAY_OF_YEAR, -1);
		Timestamp pullStart = new Timestamp(cal.getTimeInMillis());
		StringBuilder builder = new StringBuilder();
		Map<String,Map<ProgramParticipant, Integer>> special = new HashMap<>();
		List<ProgramParticipant> participants = ProgramParticipant.getParticipants(program);
		Map<String, List<Broadcast>> map = getStreams(pullStart, finish);
		for (ProgramParticipant participant: participants) {		
			if (map.containsKey(participant.STREAMER.CHANNEL) && map.get(participant.STREAMER.CHANNEL).size() > 0) {
				Map<Metric, Object> values = getMetrics(participant, map.get(participant.STREAMER.CHANNEL), start, finish);
				if (values == null) continue;
			    builder.append("\n" + participant.STREAMER.CHANNEL + "\n");
			    values.forEach((metric, value) -> {
			    	builder.append(metric + ": " + value + "\n");
			    });
			    for (ProgramRule rule: rules) {	    	
			    	if (rule.ruleCheck(values)){
			    		if (rule.getReward().contains("SPECIAL:")) {
			    			String reward = rule.getReward().replace("SPECIAL:", "");
			    			if (special.containsKey(reward)) {
				    			if (special.get(reward).containsKey(participant))
				    				special.get(reward).put(participant, new Integer(special.get(reward).get(participant).intValue() +1));
				    			else special.get(reward).put(participant, new Integer(1));
				    		}
				    		else {
				    			special.put(reward, new HashMap<>());
				    			special.get(reward).put(participant, new Integer(1));
				    		}
			    		}
			    		else ProgramReward.getOneUnassignedReward(program, rule.getReward()).assignReward(participant.STREAMER.ID, finish);
			    		builder.append(participant.STREAMER.CHANNEL + " earned " + rule.getReward() + " with " + rule.getMetric() + " = " + values.get(rule.getMetric()) + "\n");
			    	}
			    }
			}		
		}
		special.forEach((reward, part) -> {
			builder.append(reward + ":\n");
			part.forEach((p, i) -> {
				builder.append("\t" + p.STREAMER.CHANNEL + ": " + i.intValue() +"\n");
			});
		});
		RewardMailer mailer = new RewardMailer(this.program);
		mailer.sendAcctMgrMail("Results for Week ending: " + finish, builder.toString());
	}
	
	public void processLastMonth() {
		//TO DO
		//throw new UnsupportedOperationException("Not yet implemented.");
		List<ProgramRule> monthlyRules = new ArrayList<>();
		List<ProgramRule> weekInMonthRules = new ArrayList<>();
		program.getRules().forEach((k,r) -> {
			Frequency freq = r.getFrequency();
			if (freq == Frequency.MONTHLY) monthlyRules.add(r);
			else if (freq == Frequency.WEEK_IN_MONTH) weekInMonthRules.add(r);
		});
		
		TimeZone tz = TimeZone.getTimeZone("America/Los_Angeles");
		Calendar cal = Calendar.getInstance(tz);
		int thisMonth = cal.get(Calendar.MONTH);
		int lastMonth = cal.get(Calendar.MONTH) - 1;
		System.out.println(lastMonth);
		if (lastMonth < 0) lastMonth = Calendar.DECEMBER;
		cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MILLISECOND, 0);
		cal.set(Calendar.DAY_OF_MONTH, 1);
		Timestamp finish = new Timestamp(cal.getTimeInMillis());
		cal.add(Calendar.MONTH, -1);
		Timestamp start = new Timestamp(cal.getTimeInMillis());
		int offset = cal.get(Calendar.DAY_OF_WEEK)-Calendar.MONDAY;
		if (offset < 0) cal.add(Calendar.DAY_OF_YEAR, - (7+offset));
		else cal.add(Calendar.DAY_OF_YEAR, -offset);
		Timestamp weekStart = new Timestamp(cal.getTimeInMillis());
		cal.add(Calendar.DAY_OF_YEAR, -1);
		Timestamp pullStart = new Timestamp(cal.getTimeInMillis());
		List<Timestamp[]> weeks = new ArrayList<>();
		cal.add(Calendar.DAY_OF_YEAR, 8);
		while (cal.get(Calendar.MONTH) != thisMonth || (cal.get(Calendar.DAY_OF_MONTH) == 1 && cal.get(Calendar.MONTH) == Calendar.getInstance().get(Calendar.MONTH))) {
			if (cal.get(Calendar.MONTH) != lastMonth) {
				weekStart = new Timestamp(cal.getTimeInMillis());
				cal.add(Calendar.DAY_OF_YEAR, 7);
			}
			else {
				Timestamp[] thisWeek = {weekStart, new Timestamp(cal.getTimeInMillis())};
				weeks.add(thisWeek);
				weekStart = new Timestamp(cal.getTimeInMillis());
				cal.add(Calendar.DAY_OF_YEAR, 7);
			}
		}
		StringBuilder builder = new StringBuilder();
		Map<String,Map<ProgramParticipant, Integer>> special = new HashMap<>();
		List<ProgramParticipant> participants = ProgramParticipant.getParticipants(program);
		
		//Process the month
		Map<String, List<Broadcast>> map = getStreams(pullStart, finish);
		for (ProgramParticipant participant: participants) {		
			if (map.containsKey(participant.STREAMER.CHANNEL) && map.get(participant.STREAMER.CHANNEL).size() > 0) {
				Map<Metric, Object> values = getMetrics(participant, map.get(participant.STREAMER.CHANNEL), start, finish);
				if (values == null) continue;
			    builder.append("\n" + participant.STREAMER.CHANNEL + ": Monthly\n");
			    values.forEach((metric, value) -> {
			    	builder.append(metric + ": " + value + "\n");
			    });
			    for (ProgramRule rule: monthlyRules) {	    	
			    	if (rule.ruleCheck(values)){
			    		if (rule.getReward().contains("SPECIAL:")) {
			    			String reward = rule.getReward().replace("SPECIAL:", "");
			    			if (special.containsKey(reward)) {
				    			if (special.get(reward).containsKey(participant))
				    				special.get(reward).put(participant, new Integer(special.get(reward).get(participant).intValue() +1));
				    			else special.get(reward).put(participant, new Integer(1));
				    		}
				    		else {
				    			special.put(reward, new HashMap<>());
				    			special.get(reward).put(participant, new Integer(1));
				    		}
			    		}
			    		else ProgramReward.getOneUnassignedReward(program, rule.getReward()).assignReward(participant.STREAMER.ID, finish);
			    		builder.append(participant.STREAMER.CHANNEL + " earned " + rule.getReward() + " with " + rule.getMetric() + " = " + values.get(rule.getMetric()) + "\n");
			    	}
			    }
			}
			
		}
		
		//Process each individual week
		for(Timestamp[] week: weeks) {
			System.out.println(week[0]);
			map = getStreams(week[0], week[1]);
			for (ProgramParticipant participant: participants) {
				System.out.println(participant.STREAMER.CHANNEL);
				if (map.containsKey(participant.STREAMER.CHANNEL) && map.get(participant.STREAMER.CHANNEL).size() > 0) {
					Map<Metric, Object> values = getMetrics(participant, map.get(participant.STREAMER.CHANNEL), week[0], week[1]);
					if (values == null) continue;
					builder.append("\n" + participant.STREAMER.CHANNEL + ": Week " + (weeks.indexOf(week) + 1) + "\n");
				    values.forEach((metric, value) -> {
				    	builder.append(metric + ": " + value + "\n");
				    });
				    for (ProgramRule rule: weekInMonthRules) {	    	
				    	if (rule.ruleCheck(values)){
				    		if (rule.getReward().contains("SPECIAL:")) {
			    				String reward = rule.getReward().replace("SPECIAL:", "");
			    				if (special.containsKey(reward)) {
					    			if (special.get(reward).containsKey(participant))
					    				special.get(reward).put(participant, new Integer(special.get(reward).get(participant).intValue() +1));
					    			else special.get(reward).put(participant, new Integer(1));
					    		}
					    		else {
					    			special.put(reward, new HashMap<>());
					    			special.get(reward).put(participant, new Integer(1));
					    		}
				    		}
				    		else ProgramReward.getOneUnassignedReward(program, rule.getReward()).assignReward(participant.STREAMER.ID, finish);
				    		builder.append(participant.STREAMER.CHANNEL + " earned " + rule.getReward() + " with " + rule.getMetric() + " = " + values.get(rule.getMetric()) + "\n");
				    	}
				    }
				}
			}
		}
		special.forEach((reward, part) -> {
			builder.append(reward + ":\n");
			part.forEach((p, i) -> {
				builder.append("\t" + p.STREAMER.CHANNEL + ": " + i.intValue() +"\n");
			});
		});
		RewardMailer mailer = new RewardMailer(this.program);
		mailer.sendAcctMgrMail("Results for Month ending: " + finish, builder.toString());
			
	}
	private Map<Metric, Object> getMetrics(ProgramParticipant participant, List<Broadcast> broadcasts, Timestamp start, Timestamp finish) {
		Map<Metric, Object> values = new HashMap<>();
		int streamCount = 0;
		Timestamp last = null;
		values.put(Metric.BCTIME, new Long(0));
		values.put(Metric.STATUS, participant.STATUS);
		values.put(Metric.VIEWERS, new Long(0));
		values.put(Metric.STREAMS, new Long(0));
		values.put(Metric.VIEWER_MINUTES, new Long(0));
		values.put(Metric.BCTIME_PERCENTAGE, new Long(0));
		values.put(Metric.VIEWER_MINUTE_PERCENTAGE, new Long(0));
		values.put(Metric.FOLLOWER_CHANGE, new Long(0));
		values.put(Metric.VIEWER_PULL, new Long(0));
		Long totalViewerMinutes = null;
		Long totalBCTime = null;
		Connection c = null;
		try {
			c = DatabaseUrl.extract().getConnection();
			PreparedStatement s = c.prepareStatement("SELECT COUNT(timestamp) as ct, AVG(viewers) as avg FROM streams WHERE channel = ? AND timestamp >= ? AND timestamp <= ?");
			s.setString(1, participant.STREAMER.CHANNEL);
			s.setTimestamp(2, start);
			s.setTimestamp(3, finish);
			ResultSet rs = s.executeQuery();
			if (rs.next()) {
				totalBCTime = new Long(rs.getLong("ct") * 6);
				totalViewerMinutes = new Long(totalBCTime.longValue() * rs.getLong("avg"));
			}
			rs.close();
			s.close();
			PreparedStatement t = c.prepareStatement(
					"SELECT b.timestamp, b.followers FROM (SELECT MAX(timestamp) as max, MIN(timestamp) as min FROM streams WHERE channel = ? AND timestamp >= ? AND timestamp <= ?) a INNER JOIN (SELECT followers, timestamp FROM streams WHERE channel = ?) b ON a.max = b.timestamp OR a.min = b.timestamp ORDER BY b.timestamp");
			t.setString(1, participant.STREAMER.CHANNEL);
			t.setTimestamp(2, start);
			t.setTimestamp(3, finish);
			t.setString(4, participant.STREAMER.CHANNEL);
			ResultSet ts = t.executeQuery();
			long firstF = 0;
			if (ts.next()) {
				firstF = ts.getLong("followers");
				if (ts.next()) values.put(Metric.FOLLOWER_CHANGE, ts.getLong("followers") - firstF);
			}
			ts.close();
			t.close();
		} catch (Exception e){			
			System.out.println(e.getMessage());
			e.printStackTrace(System.out);
		} finally {
			if (c != null) try{c.close();} catch(SQLException e){}
		}
		long vsum = 0;
	    long pullSum = 0;
	    long vmSum = 0;
	    int castCount = 0;
	    for (Broadcast evt: broadcasts) {
	    	if ( evt.getStart().getTime() > start.getTime() ) {
	    		values.put(Metric.BCTIME, new Long(((Long)values.get(Metric.BCTIME)).longValue() + evt.getLength()));
	    		vsum += evt.avgViewers();
	    		vmSum += evt.getLength()*evt.avgViewers();
	    		pullSum += evt.getRampUp();
	    		castCount++;
	    		if ((last == null || evt.getStart().getTime() - last.getTime() > (long)1000*60*60*12) && evt.getLength() > 24) {
	    			streamCount++;
			    	last = evt.getStart();
	    		}
	    	}
	    }
	    if (totalBCTime == 0 || castCount == 0) return null;
	    values.put(Metric.STREAMS, new Long(streamCount));
	    values.put(Metric.BCTIME_PERCENTAGE, new Long(((Long)values.get(Metric.BCTIME)).longValue()*100/totalBCTime));
	    values.put(Metric.VIEWERS, new Long(vsum/castCount));
	    values.put(Metric.VIEWER_MINUTES, new Long(vmSum));
	    values.put(Metric.VIEWER_MINUTE_PERCENTAGE, new Long(vmSum*100/totalViewerMinutes));
	    values.put(Metric.VIEWER_PULL, pullSum);
	    return values;
	}
	
	private Map<String,List<Broadcast>> getStreams(Timestamp start, Timestamp finish) {
		List<ProgramParticipant> pp = ProgramParticipant.getParticipants(this.program);
		Map<String, List<Broadcast>> map = new HashMap<>();
		if (pp.size() < 30) return getStreams(pp, start, finish);
		else {
			for (int i = 0; i < pp.size(); i+=30) {
				int x = i+(30-1);
				if (x >= pp.size()) x = pp.size()-1;
				map.putAll(getStreams(pp.subList(i, x), start, finish));
			} 
		}
		return map;
	}
	
	private Map<String,List<Broadcast>> getStreams(List<ProgramParticipant> pps, Timestamp start, Timestamp finish) {
		if (pps.size() > 30) {
			throw new IllegalArgumentException("Too many channels.");
		}

		Connection connection = null;
		try {
            connection = DatabaseUrl.extract().getConnection();
            String psBuild = "SELECT channel, game, timestamp, viewers, followers FROM streams WHERE timestamp >= ? AND timestamp <= ? AND (channel = ? ";
            for (int i = 1; i < pps.size(); i++) {
            	psBuild = psBuild + "OR channel = ? ";
            }
            psBuild = psBuild + ") AND ( game = ? ";
            for (int i = 1; i < this.program.GAMES.length; i++) {
            	psBuild = psBuild + "OR game = ? ";
            }
            psBuild = psBuild +	") ORDER BY channel, timestamp ASC";
            PreparedStatement ps = connection.prepareStatement(psBuild);
        	
        	ps.setTimestamp(1, start);
        	ps.setTimestamp(2, finish);
        	int pos = 3;
        	for (ProgramParticipant pp: pps) {
        		ps.setString(pos++, pp.STREAMER.CHANNEL);
        	}
        	for (String s: this.program.GAMES) {
        		ps.setString(pos++, s);
        	}
            ResultSet rs = ps.executeQuery();
            Map<String, List<Broadcast>> map = new HashMap<>();

            List<Broadcast> events = new ArrayList<>();
            Broadcast current = null;
            String currentChannel = null;
            while (rs.next()) {
            	
            	if (currentChannel == null) currentChannel = rs.getString("channel");
            	StreamSnapshot ss = new StreamSnapshot(rs.getString("channel"), rs.getTimestamp("timestamp"), rs.getString("game"), rs.getLong("viewers"), rs.getLong("followers"));
            	if (!currentChannel.equals(rs.getString("channel"))) {
            		map.put(currentChannel, events);
            		events = new ArrayList<>();
            		currentChannel = rs.getString("channel");
            		current = new Broadcast(ss);
            		events.add(current);
            		continue;
            	}
            	if (current == null || ss.TIME.getTime() - 1860000 > current.getEnd().getTime()) {
            		current = new Broadcast(ss);
            		events.add(current);
            	}
            	else current.addSnapShot(ss);
            }
            map.put(currentChannel, events);
            rs.close();

            return map;

		} catch (Exception e){			
			System.out.println(e.getMessage());
			e.printStackTrace(System.out);
			return null;
		} finally {
			if (connection != null) try{connection.close();} catch(SQLException e){}
		}
		
	}

}
