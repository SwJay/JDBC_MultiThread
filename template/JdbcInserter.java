package template;

import template.JdbcBase;

import java.sql.*;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

public class JdbcInserter extends JdbcBase {

	public JdbcInserter(String dbUrl, String user, String password) {
		super(dbUrl, user, password);
	}

	public void singleThreadInsert(long total) {
		insert_delete(total,true);
	}

	public void multiThreadInsert(long total, int thread) {
		CountDownLatch countDownLatch = new CountDownLatch(thread);

		long startTime = System.nanoTime();
		for(int i=0;i<thread;i++){
			new Thread(()->{
				Connection connection = getDbConnection();
				try{
					Statement statement = connection.createStatement();
					connection.setAutoCommit(false);
					for(int j=0;j<total/thread;j++){
						String uuid = UUID.randomUUID().toString();
						String insertStatement = "INSERT INTO test(uuid) values ('" + uuid + "')";
						statement.addBatch(insertStatement);
						if(j%10000==0){
							statement.executeBatch();
							connection.commit();
						}
					}
					statement.executeBatch();
					connection.commit();
					countDownLatch.countDown();
					double processTime = (double)(System.nanoTime() - startTime)/1000000;
					System.out.println("Process Time: " + processTime + "ms");
					connection.close();
					statement.close();
				} catch (SQLException e) {
					System.out.println("ERROR");
					e.printStackTrace();
				}
			}).start();
		}
	}

	public void delete(long total) {
		insert_delete(total,false);
	}
}
