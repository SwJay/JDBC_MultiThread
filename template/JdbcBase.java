package template;

import java.sql.*;
import java.util.UUID;

public class JdbcBase {

	protected final String dbUrl;
	protected final String user;
	protected final String password;

	public JdbcBase(String dbUrl, String user, String password) {
		this.dbUrl = dbUrl;
		this.user = user;
		this.password = password;
	}

	/* Get database connection object */
	protected Connection getDbConnection() {
		Connection conn = null;
		try {
			conn = DriverManager.getConnection(dbUrl, user, password);
			System.out.println("database is connected.\n");
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return conn;
	}

	protected void insert_delete(long total, boolean operation) {
		int idid = 0;
		Connection connection = null;
		Statement statement = null;

		try {
			// Step 1: Allocate a database 'Connection' object
			connection = getDbConnection();

			// Step 2: Allocate a 'Statement' object in the Connection
			statement = connection.createStatement();
			String Statement1,Statement2, Statement3;
			long startTime = System.nanoTime();
			if(operation==true){ // insert
				Statement1 = "INSERT INTO test(UUID) VALUES ('";
				Statement2 = "')";
			}
			else{ // delete
				Statement1 = "DELETE FROM test WHERE id = '";
				Statement2 = "' and uuid <>'";
				Statement3 = "'";
			}

			for(long id = 1; id <= total; id++) {
				String uuid = UUID.randomUUID().toString();
				String insertStatement = Statement1 + uuid + Statement2;
				//System.out.println(insertStatement);
				// Step 3: Execute a SQL INSERT statement
				statement.execute(insertStatement);
			}

			double processTime = (double)(System.nanoTime() - startTime)/1000000;
			System.out.println("Process Time: " + processTime + "ms");
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			// Step 4: Close the resources
			try {
				if (statement != null)
					statement.close();
				if (connection != null)
					connection.close();
			} catch (SQLException se) {
				se.printStackTrace();
			}
		}
	}

	protected void select() {

		String query = "SELECT * FROM `test`";

		Connection connection = null;
		Statement statement = null;
		ResultSet resultSet = null;

		try {
			// Step 1: Allocate a database 'Connection' object
			connection = DriverManager.getConnection(dbUrl, user, password);

			// Step 2: Allocate a 'Statement' object in the Connection
			statement = connection.createStatement();

			// Echo For debugging
			System.out.println("The SQL query is: \"" + query + "\"");

			// Step 3: Execute a SQL SELECT query, the query result is returned in a 'ResultSet' object.
			resultSet = statement.executeQuery(query);

			System.out.println("The records selected are:");
			int rowCount = 0;
			long startTime = System.nanoTime();
			// Step 4: Process the ResultSet by scrolling the cursor forward via next().
			while(resultSet.next()) {  // Move the cursor to the next row, return false if no more row
				// For each row, retrieve the contents of the cells with getXxx(columnName).
				Long id = resultSet.getLong("id");
				String uuid = resultSet.getString("uuid");
				//System.out.println("\t" + id + ", " + uuid);
				rowCount++;
			}

			double processTime = (double)(System.nanoTime() - startTime)/1000000;
			System.out.println("Total number of records = " + rowCount);
			System.out.println("[SELECT] Process Time: " + processTime + "ms");
		} catch(SQLException e) {
			e.printStackTrace();
		} finally {
			// Step 5: Close the resources
			try {
				if (resultSet != null)
					resultSet.close();
				if (statement != null)
					statement.close();
				if (connection != null)
					connection.close();
			} catch (SQLException se) {}
		}
	}
}
