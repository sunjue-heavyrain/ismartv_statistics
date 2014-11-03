package com.ismartv.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class HiveConnection {
	// private static String driverName = "org.apache.hive.jdbc.HiveDriver";
	private static String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";
	private static Connection conn = null;

	private HiveConnection() {
	}

	public static Connection getHiveConnection() throws SQLException,
			ClassNotFoundException {
		if (conn == null) {
			Class.forName(driverName);
			// conn = DriverManager.getConnection(
			// "jdbc:hive2://10.0.4.11:10000/default", "deploy", "");
			conn = DriverManager.getConnection(
					"jdbc:hive://10.0.4.11:10000/default", "", "");
			return conn;
		}
		return conn;
	}

	public static void close() throws SQLException {
		if (conn != null) {
			conn.close();
		}
	}
}
