package general;
import org.apache.mahout.cf.taste.common.TasteException;
//import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
//import org.apache.mahout.cf.taste.impl.model.file.*;  
//import org.apache.mahout.cf.taste.impl.neighborhood.*;  
//import org.apache.mahout.cf.taste.impl.recommender.*;  
//import org.apache.mahout.cf.taste.impl.similarity.*;
//import org.apache.mahout.cf.taste.model.*;   

import dao.connection;

import java.io.*;  
//import java.util.*;  
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class Experiment {

//  final static int NEIGHBORHOOD_NUM = 10;
//  final static int RECOMMENDER_NUM = 3;

  public static void main(String[] args) throws IOException, TasteException {
	  new dao.connection();
		
		Connection conn = connection.getDao();
		Statement stmt = null;
		   
		try{
			System.out.println("Creating table in given database...");
		    stmt = conn.createStatement();
		      
		    String sql = "CREATE TABLE user_neighborhood (" +
		                 " user_id Integer, "+
		                 " knowledge_id Integer, " + 
		                 " isWrong Double, " +
		                 " method Integer, " +
		                 " PRIMARY KEY ( user_id ))"; 

		    stmt.executeUpdate(sql);
		    System.out.println("Created table in given database...");
		 }catch(SQLException se){
		    //Handle errors for JDBC
		    se.printStackTrace();
		 }catch(Exception e){
		    //Handle errors for Class.forName
		    e.printStackTrace();
		 }finally{
		    //finally block used to close resources
		    try{
		       if(stmt!=null)
		          conn.close();
		    }catch(SQLException se){
		    }// do nothing
		    try{
		       if(conn!=null)
		          conn.close();
		    }catch(SQLException se){
		       se.printStackTrace();
		    }
		 }
	}
}
