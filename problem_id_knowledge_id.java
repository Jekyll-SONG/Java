package com.xie;

import java.sql.*;
//import java.io.BufferedWriter; 
//import java.io.File; 
//import java.io.FileNotFoundException; 
//import java.io.FileWriter; 
//import java.io.IOException; 
//import java.util.*; 

public class problem_id_knowledge_id {
	static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";  
	static final String DB_URL = "jdbc:mysql://localhost:3306/knowledge_map";

	//  Database credentials
	static final String USER = "root";
	static final String PASS = "lxsjw495";
	
	public static void main(String[] args) {
		   Connection conn = null;
		   Statement stmt = null;
		   Statement stmt2 = null;
		   try{
		      //STEP 2: Register JDBC driver
		      Class.forName("com.mysql.jdbc.Driver");

		      //STEP 3: Open a connection
		      System.out.println("Connecting to a selected database...");
		      conn = DriverManager.getConnection(DB_URL, USER, PASS);
		      System.out.println("Connected database successfully...");
		      
		      //STEP 4: Execute a query
		      
		      System.out.println("Inserting into table");
		      stmt = conn.createStatement();
		      
		      String sql="select R.user_id as user_id, K.knowledge_id as knowledge_id, R.date as date, R.isWrong as isWrong "+
		    		  		" from record R, knowledge_problem K where R.problem_id=K.problem_id order by user_id";
	    	  
		      ResultSet result=stmt.executeQuery(sql);
		      
		      stmt2 = conn.createStatement();
	    	  
	    	  
	    	  while(!result.wasNull())
	    	  {
	    		  sql="insert into user_knowledge (user_id, knowledge_id, date, isWrong) values ";
	    		  boolean change=false;
	    		  int m=0;
		    	  for(int i =0; i<1000 && result.next(); i++)

//	    		  while(result.next())
		    	  {
		    		  Timestamp time=result.getTimestamp("date");
		    		  sql+=" ("+result.getInt("user_id")+", "+result.getInt("knowledge_id")+", '"+time+"', "+result.getInt("isWrong")+"),";
//		    		  change=true;
		    		  System.out.println(m);
		    		  m++;
		    	  }
		    	  
		    	  //eliminate the comma at the end of the string
		    	  if (sql.charAt(sql.length()-1)==',') {
		    		  sql=sql.substring(0, sql.length()-1);
		    	  }
		    	  
		    	  //check whether there is any change to the string
		    	  if(change){
		    		  stmt2.executeUpdate(sql);
		    	  }
	    	  }
		      System.out.println("Inserted into the table with given database...");
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
		      }//end finally try
		   }//end try
		   System.out.println("Goodbye!");
		}//end main
}
