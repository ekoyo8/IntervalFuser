import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

//1: Get the largest interval from starting date.
//2: Iteratively find the largest possible time interval where the start-date is '<=' the end-date of the previous iteration.
//3: When no new results are found, conclude the interval by outputting it.
//4: Begin fusing a new interval, by initially looking for the earliest starting date that is also larger than than the end date of the previous interval.

public class IntervalFuser {
	private String startLimit = "2000-01-01 00:00:00.000000"; //default value.
	private String endLimit = "2345-01-01 00:00:00.000000"; //default value.
	private Integer idLimit = 0; //default value.
	
	private String intervalStart = startLimit;
	private String intervalEnd = null;
	private boolean initialIteration = true;
	private boolean newInterval = true;
	private Connection conn;
	private PreparedStatement prepStatement;
	private ResultSet rs;
	
	public IntervalFuser(String startLimit, String endLimit, int idLimit) {
		this.startLimit = startLimit;
		this.endLimit = endLimit;
		this.idLimit = idLimit;
	}
	
	public IntervalFuser() {}
	
	public void fuseIntervals() {
		do {
			if (newInterval) {
				//1: Get the largest interval from starting date.
				try {
					//Build query
					StringBuilder initialQuery = new StringBuilder(
							"SELECT data_since, MAX(data_until), COUNT(data_since) FROM data_events "
							+ "WHERE data_since = (SELECT MIN(data_since) FROM data_events");
					if (intervalStart != null || intervalStart.isEmpty() == false) {
						if (initialIteration) {
							initialQuery.append(" WHERE data_since >= ?");
							initialIteration = false;
						} else {
							initialQuery.append(" WHERE data_since > ?");
						}
					}
					
					initialQuery.append(")");
					if (endLimit != null || endLimit.isEmpty() == false) {
						initialQuery.append(" AND data_until <= ?");
					}
					
					if (idLimit != null) {
						initialQuery.append(" AND id >= ?");
					}
					
					initialQuery.append(";");
					//(USING LIMITING ARGUMENTS): SELECT data_since, MAX(data_until), COUNT(data_since) FROM data_events WHERE data_since = (SELECT MIN(data_since) FROM data_events WHERE data_since >= ?) AND data_until <= ? AND id >= ?; //omit the where clause in the nested query, if no startLimit argument was given.
					//(USING LIMITING ARGUMENTS) EXAMPLE: SELECT data_since, MAX(data_until), COUNT(data_since) FROM data_events WHERE data_since = (SELECT MIN(data_since) FROM data_events WHERE data_since >= '2016-01-01 00:00:00.000000') AND data_until <= '2017-04-01 00:00:00.000000' AND id >= 0;
					//Execute query
					conn = DriverManager.getConnection("jdbc:sqlite:data_events.sqlite");
					prepStatement = conn.prepareStatement(initialQuery.toString());
					int paramCount = 0;
					if (intervalStart != null || intervalStart.isEmpty() == false) {
						paramCount++;
						prepStatement.setString(paramCount, intervalStart);
					}
					
					if (endLimit != null || endLimit.isEmpty() == false) {
						paramCount++;
						prepStatement.setString(paramCount, endLimit);
					}
					
					if (idLimit != null) {
						paramCount++;
						prepStatement.setInt(paramCount, idLimit);
					}
					
					rs = prepStatement.executeQuery();
					rs.next();
					if (rs.getInt(3) == 0) { //TODO: Find out why the ResultSet can return an "empty" row, even without the 'COUNT(data_since)'-column.
						break; //No further intervals found.
					}
					
					intervalStart = rs.getString(1); //date_since from the ResultSet.
					intervalEnd = rs.getString(2); //MAX(data_until) from the ResultSet.
				} catch (Exception e) {
					e.printStackTrace();
				} finally {
					try {if (rs != null) rs.close();} catch (SQLException e) {}
					try {if (prepStatement != null) prepStatement.close();} catch (SQLException e) {}
					try {if (conn != null) conn.close();} catch (SQLException e) {}
				}
				
				newInterval = false;
				continue;
			}
			
			//2: Iteratively find the largest possible time interval where the start-date is '<=' the end-date of the previous iteration.
			try {
				StringBuilder iterativeQuery = new StringBuilder(
						"SELECT MAX(data_until), COUNT(data_until) FROM data_events "
						+ "WHERE data_since > ? "
						+ "AND data_since <= ? "
						+ "AND data_until > ? "
						+ "AND data_until <= ? "
						+ "AND id >= ?;");
				//(USING LIMITING ARGUMENTS): SELECT MAX(data_until), COUNT(data_until) FROM data_events WHERE data_since > ? AND data_since <= ? AND data_until > ? AND data_until <= ? AND id >= ?;
				//(USING LIMITING ARGUMENTS) EXAMPLE: SELECT MAX(data_until), COUNT(data_until) FROM data_events WHERE data_since > '2015-01-01 00:00:00.000000' AND data_since <= '2017-06-17 00:00:00.000000' AND data_until > '2017-06-17 00:00:00.000000' AND data_until <= '2017-11-21 00:00:00.000000' AND id >= 0;
				conn = DriverManager.getConnection("jdbc:sqlite:data_events.sqlite");
				prepStatement = conn.prepareStatement(iterativeQuery.toString());
				prepStatement.setString(1, intervalStart);
				prepStatement.setString(2, intervalEnd);
				prepStatement.setString(3, intervalEnd);
				prepStatement.setString(4, endLimit);
				prepStatement.setInt(5, idLimit);
				rs = prepStatement.executeQuery();
				if (rs.next() && rs.getInt(2) > 0) { //TODO: Find out why the ResultSet can return an empty row, even without the 'COUNT(data_until)'-column.
					intervalEnd = rs.getString(1); //MAX(data_until) from the ResultSet.
				} else {
					//3: When no new results are found, conclude the interval by outputting it.
					//Write interval as "[intervalStart , intervalEnd[".
					System.out.println("[" + intervalStart + " , " + intervalEnd + "[");
					
					//4: Begin fusing a new interval, by initially looking for the earliest starting date that is also larger than than the end date of the previous interval.
					intervalStart = intervalEnd;
					newInterval = true;
				}
				
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				try {if (rs != null) rs.close();} catch (SQLException e) {}
				try {if (prepStatement != null) prepStatement.close();} catch (SQLException e) {}
				try {if (conn != null) conn.close();} catch (SQLException e) {}
			}
			
		} while (intervalEnd != null && 
				intervalEnd.compareTo(endLimit) < 0);
		
		System.out.println("[END]");
	}

	public String getStartLimit() {
		return startLimit;
	}

	public void setStartLimit(String startLimit) {
		this.startLimit = startLimit;
	}

	public String getEndLimit() {
		return endLimit;
	}

	public void setEndLimit(String endLimit) {
		this.endLimit = endLimit;
	}

	public Integer getIdLimit() {
		return idLimit;
	}

	public void setIdLimit(Integer idLimit) {
		this.idLimit = idLimit;
	}
	
}
