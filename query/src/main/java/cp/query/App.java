package cp.query;

import java.sql.SQLException;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;

public class App {
  private static String driverName = "org.apache.hive.jdbc.HiveDriver";
 
  /**
   * @param args
   * @throws SQLException
 * @throws UnsupportedEncodingException 
 * @throws FileNotFoundException 
   */
  public static void main(String[] args) throws SQLException, FileNotFoundException, UnsupportedEncodingException {
	  try {
		  Class.forName(driverName);
	  } catch (ClassNotFoundException e) {
		  e.printStackTrace();
		  System.exit(1);
	  }	
	  
	  String[] tokens = args[0].split("\\s");
	  
	  Connection conn = DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "s103062512", "cm411in9413");
	  
	  Statement stmt = conn.createStatement();
	  String invertedIndexTable = "invertedindex";
	  String pageRankTable = "pagerank";
	  
	  PrintWriter writer = new PrintWriter("the-file-name.txt", "UTF-8");
	  
	  ResultSet res = stmt.executeQuery("SELECT * FROM invertedindex WHERE word = '" +  + "'");
	  while (res.next()) {
		  writer.println(res.getString(1));
	  }
	  
	  res.close();
	  stmt.close();
	  conn.close();
	  writer.close();
	  System.exit(0);
  }
}
