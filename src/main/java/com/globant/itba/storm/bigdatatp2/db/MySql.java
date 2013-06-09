package com.globant.itba.storm.bigdatatp2.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;


public class MySql {

	
	public void insertRow(String metric_type, long minuteFromEpoch, String key, long quantity){
		try {
		Connection connection = this.connect();
			Statement statement = connection.createStatement();
			statement.execute("INSERT INTO metrics(METRIC_TYPE, METRIC_KEY, MINUTE, QUANTITY) VALUES ('"+metric_type+"','"+key+"',"+String.valueOf(minuteFromEpoch)+","+String.valueOf(quantity)+");");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private Connection connect() throws SQLException, ClassNotFoundException {
		Class.forName("com.mysql.jdbc.Driver");
		Connection conn = null;
		conn = DriverManager.getConnection("jdbc:mysql://10.212.83.136:3306/bigdata",
				"root", "root");
		return conn;
	}
}
