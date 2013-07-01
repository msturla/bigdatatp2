package com.globant.itba.storm.bigdatatp2.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;


public class MySql {
	

	
	public static void insertRow(Connection con, String metric_type, long minuteFromEpoch, String key, long quantity){
		try {
			Statement statement = con.createStatement();
			statement.execute("INSERT INTO "+ metric_type +"(METRIC_KEY, MINUTE, QUANTITY) VALUES ('"+key+"',"+String.valueOf(minuteFromEpoch)+","+String.valueOf(quantity)+");");
		} catch (Exception e) {
			e.printStackTrace();
		}
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
