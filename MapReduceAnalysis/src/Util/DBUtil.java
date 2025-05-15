package Util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DBUtil {

	public static final String driverClass = "com.mysql.cj.jdbc.Driver";
	public static final String dbUrl = "jdbc:mysql://192.168.1.100:3306/db_cartool_project?useUnicode=true&characterEncoding=UTF8";
	public static final String userName = "root";
	public static final String passwd = "123456";
 
	public static Connection conn = null;
	
	public static Connection getConnection() {
		
		try {
			Class.forName(driverClass);
			conn = DriverManager.getConnection(dbUrl,userName,passwd);
		}catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		return conn;
	}
	
	public static void close(Connection c, java.sql.Statement s, ResultSet r)
			throws SQLException {
		if (c != null) {
			c.close();
		}
		if (s != null) {
			s.close();
		}
		if (r != null) {
			r.close();
		}
	}
	
}
