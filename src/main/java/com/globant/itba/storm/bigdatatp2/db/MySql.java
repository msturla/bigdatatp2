package com.globant.itba.storm.bigdatatp2.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.Logger;


public class MySql {
	
	private static Logger LOG = Logger.getLogger(MySql.class);
	
	
	public static void insertRow(Connection con, String metric_type, long minuteFromEpoch, String key, long quantity){
		try {
			Statement statement = con.createStatement();
			statement.execute("INSERT INTO "+ metric_type +"(METRIC_KEY, MINUTE, QUANTITY) VALUES ('"+key+"',"+String.valueOf(minuteFromEpoch)+","+String.valueOf(quantity)+");");
		} catch (Exception e) {
			LOG.error("Error while persisting data from MySql: " + e.getMessage());
		}
	}
	
	public static long getValue(Connection con, String metric_type, long minuteFromEpoch, String key) throws SQLException {
			Statement statement = con.createStatement();
			statement.execute(String.format("SELECT QUANTITY FROM  %s WHERE METRIC_KEY = '%s' AND MINUTE = %d;",
					metric_type, key, minuteFromEpoch));
			ResultSet results = statement.getResultSet();
			if (!results.first()) {
				LOG.warn("Asked for allegedly persisted values but none where found.");
				return 0;
			}
			return results.getLong(1);
	}
	
	public static Connection connect() throws SQLException, ClassNotFoundException {
		Class.forName("com.mysql.jdbc.Driver");
		Connection conn = null;
    	 
    		String connectionString = "jdbc:mysql://54.224.21.206:3306/bigdata";
    		conn = DriverManager.getConnection(connectionString,
					"root", "root");    		
 
    	
		return conn;
	}
}
