package edu.uwm.elsevier.namedisambiguation;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import articlesdata.database.DBConnection;

public class AuthorityDatabaseConnection implements DBConnection{
	private static AuthorityDatabaseConnection singleton = null;
//  public static final String URL = "jdbc:mysql://127.0.0.1:3306/articles_data_2";
  public static final String URL = "jdbc:mysql://compute-0-10:3306/authority";
//  
//  public static final String URL = "jdbc:mysql://10.1.1.200:3306/authority"; // snake
  public static final long CONNECTION_STALE_TIME = 3600000; // Connection will be considered stale after 1 hour
  
  private Connection con = null;
  private Properties properties;
  private long connectionAtTime;
//  private Statement stmt = null;

  private AuthorityDatabaseConnection() throws ClassNotFoundException, SQLException {
      if (con != null) {
          return;
      }
      Class.forName(MYSQL_DRIVER_CLASS);
      properties = new Properties();
      properties.setProperty("user", "qzhang");
      properties.setProperty("password", "uwmbionlp");
//      properties.setProperty("user", "cate");
//      properties.setProperty("password", "better");
      properties.setProperty("autoReconnect", "true");
      con = DriverManager.getConnection(URL, properties);
      connectionAtTime = System.currentTimeMillis();
//      stmt = con.createStatement();
  }

  /**
   * Gets an instance of {@link DataBaseConnection}
   * @return an instance of {@link DataBaseConnection}
   * @throws ClassNotFoundException in the event of an error
   * @throws SQLException in the event of an error
   */
  public static AuthorityDatabaseConnection getInstance() throws ClassNotFoundException, SQLException {
      if (singleton != null) {
          return singleton;
      }
      singleton = new AuthorityDatabaseConnection();
      return singleton;
  }
  
  @Override
  public Statement getStatement() throws SQLException {
      return getConnection().createStatement();
  }

  @Override
  public Connection getConnection() throws SQLException {
      // Check if connection time increases stale time. If it does, reconnect.
      // This will help avoid 
      // com.mysql.jdbc.exceptions.jdbc4.MySQLNonTransientConnectionException: 
      // Connection.close() has already been called
      // exception
      long currentTime = System.currentTimeMillis();
      if (currentTime - connectionAtTime > CONNECTION_STALE_TIME) {
          // Close old connection
          if (con != null) {
              con.close();
          }
          // Create new connection
          con = DriverManager.getConnection(URL, properties);
      }
      
      // Check if connection exists, if not, create a new connection
      if (con == null || con.isClosed()) {
          con = DriverManager.getConnection(URL, properties);
      } else if (!con.isValid(10)) {
//          con.commit();
//          con.close();
          con = DriverManager.getConnection(URL, properties);
      }
      
      // Refresh connection at time, since it is valid right now
      connectionAtTime = System.currentTimeMillis();
      return con;
  }

  @Override
  public void close() throws SQLException {
      con.close();
  }
}
