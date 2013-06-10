package com.globant.itba.storm.bigdatatp2.db;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;


public class MySql {

	
	public void insertRow(String metric_type, long minuteFromEpoch, String key, long quantity){
		try {
		Connection connection = this.connect();
			Statement statement = connection.createStatement();
			statement.execute("INSERT INTO "+ metric_type +"(METRIC_KEY, MINUTE, QUANTITY) VALUES ('"+key+"',"+String.valueOf(minuteFromEpoch)+","+String.valueOf(quantity)+");");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private Connection connect() throws SQLException, ClassNotFoundException {
		Class.forName("com.mysql.jdbc.Driver");
		Connection conn = null;
    	Properties prop = new Properties();
    	 
    	try {
    		prop.load(new FileInputStream("db.properties"));
    		String connectionString = "jdbc:mysql://10.212.83.136:3306/" + prop.getProperty("database");
    		conn = DriverManager.getConnection(connectionString,
					prop.getProperty("dbuser"), prop.getProperty("dbpassword"));    		
 
    	} catch (IOException ex) {
    		ex.printStackTrace();
        }
		return conn;
	}
}
