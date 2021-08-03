package test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

public class TestDAO {
	Connection conn = null;
	PreparedStatement pstmt = null;
	
    String host = "192.168.99.223:8888";
    String dbname = "test";
    String url = "jdbc:mariadb://" + host + "/" + dbname;
    String username = "maxscale";
    String password = "maxscale";
	    
	void connect() {
		try {
			Class.forName("org.mariadb.jdbc.Driver");
			conn = DriverManager.getConnection(url, username, password);
		} catch (Exception e) {
			e.printStackTrace();
		} 
	}
	
	void disconnect() {
		if(pstmt != null) {
			try {
				pstmt.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		if(conn != null) {
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	public ArrayList<testbean> selectDB() {
		connect();
		String sql = "SELECT *, sleep(1) FROM t1 where t = 'bb'";
		ArrayList<testbean> list = new ArrayList<>();
		testbean bean = null;

		try {
			pstmt = conn.prepareStatement(sql);
			//pstmt = conn.prepareStatement(sql, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
			ResultSet rs = pstmt.executeQuery(sql);
			while (rs.next()) {
				bean = new testbean();
				bean.set_t(rs.getString("t"));
				list.add(bean);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			disconnect();
		}
		return list;
	}
	
	public boolean insertDB(String testbean) {
		connect();
		String sql = "insert into test.t1 (t)" + "values (?)";
		
		try {
			pstmt = conn.prepareStatement(sql);
			//pstmt = conn.prepareStatement(sql, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
			pstmt.setString(1, testbean);
				
			pstmt.executeUpdate();

		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		} finally {
			disconnect();
		}
		return true;
	}	
	
	public boolean setDB(String testbean) {
		connect();
		String sql = "set session sql_mode='?'";
		
		try {
			pstmt = conn.prepareStatement(sql);
			//pstmt = conn.prepareStatement(sql, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
			pstmt.setString(1, testbean);
				
			pstmt.executeUpdate();

		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		} finally {
			disconnect();
		}
		return true;
	}	
	
}
