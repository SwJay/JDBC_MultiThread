package template;

import template.JdbcBase;
import java.sql.*;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

public class JdbcSelector extends JdbcBase {

	private long startPk = 1;
	private long endPk = Long.MAX_VALUE;

	public JdbcSelector(String dbUrl, String user, String password) {
		super(dbUrl, user, password);
	}

	private String getPkName() {
		return null;
	}

	public void singleThreadSelect(String query) {
		select();
	}

	public void multiThreadSelect(int thread, long total) {
		CountDownLatch countDownLatch = new CountDownLatch(thread);

		long startTime = System.nanoTime();
		for(int i=0;i<thread;i++){
			int finalI = i;
			new Thread(()->{
				Connection connection = getDbConnection();
				String s1 = "SELECT * FROM test where id > ";
				String s2 = " and id < ";
				//String s3 = "";
				try {
					Statement statement = connection.createStatement();
					String sqlstatement = s1 + finalI *total/thread + s2 + (finalI+1) *total/thread; //+ s3;
					//System.out.println(sqlstatement);
					ResultSet rs = statement.executeQuery(sqlstatement);//获取结果集

					int rowCount = 0;
					while(rs.next()) {  // Move the cursor to the next row, return false if no more row
						// For each row, retrieve the contents of the cells with getXxx(columnName).
						Long id = rs.getLong("id");
						String uuid = rs.getString("uuid");
						//System.out.println("\t" + id + ", " + uuid);
						rowCount++;
					}
					statement.close();
					connection.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
				double processTime = (double)(System.nanoTime() - startTime)/1000000;
				System.out.println("Process Time: " + processTime + "ms");
			}).start();
		}
	}
}
