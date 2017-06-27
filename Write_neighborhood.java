package com.xie;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
//import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.impl.model.file.*;  
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood;
//import org.apache.mahout.cf.taste.impl.neighborhood.*;  
//import org.apache.mahout.cf.taste.impl.recommender.*;  
import org.apache.mahout.cf.taste.impl.similarity.*;  
import org.apache.mahout.cf.taste.model.*;   
//import org.apache.mahout.cf.taste.recommender.*;  
import org.apache.mahout.cf.taste.similarity.*;  

import dao.connection;

import java.io.*;  
import java.sql.Connection;
//import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
//import java.util.*;  

public class Write_neighborhood {

    final static int NEIGHBORHOOD_NUM = 10;
//    final static int RECOMMENDER_NUM = 3;


    public void gen_neighborhood() throws IOException, TasteException{
		new dao.connection();
		
		Connection conn = connection.getDao();
		Statement stmt = null;
        String file = "src/data/testCF.csv";
        
        //when duplicate, this function uses override method
        DataModel model = new FileDataModel(new File(file));
        
        //this calculate method brings problem
        UserSimilarity user = new EuclideanDistanceSimilarity(model);
        
        //create the 10 nearest neighbors
        NearestNUserNeighborhood neighbor = new NearestNUserNeighborhood(NEIGHBORHOOD_NUM, user, model);
        
		try{
		      //STEP 2: Register JDBC driver
		      Class.forName("com.mysql.jdbc.Driver");

		      //STEP 3: Open a connection
		      System.out.println("Connecting to a selected database...");
		      System.out.println("Connected database successfully...");
		      
		      //STEP 4: Execute a query
		      System.out.println("Creating table in given database...");
		      stmt = conn.createStatement();
		      
		      LongPrimitiveIterator iter=model.getUserIDs();
     
		      while(iter.hasNext()){
			      String sql="insert into user_neighborhood (user_id, neighbor_id, similarity) values";
		    	  
			      for(int m=0; m<50&&iter.hasNext(); m++){
			    	  String string_id=Long.toString(iter.next());
			    	  int user_id=Integer.parseInt(string_id);
			      
			    	  long neighbor_id[]=neighbor.getUserNeighborhood(user_id);
			      

			    	  for(int j=0; j<NEIGHBORHOOD_NUM; j++){
			    		  sql+=" ("+user_id+", "+neighbor_id[j]+", "+user.userSimilarity(user_id, neighbor_id[j])+" ),";
			    	  }
			      }
			      
	    	      sql= sql.substring(0, sql.length()-1);
			      
			      stmt.executeUpdate(sql);
		      }
		      
		      System.out.println("neighbor has been inserted");

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
		   }
    }
    
    public void create_neighborhood_table(){
		new dao.connection();
		
		Connection conn = connection.getDao();
		Statement stmt = null;
    		
		try{

    		      
			//STEP 4: Execute a query
    		System.out.println("Creating table in given database...");
    		stmt = conn.createStatement();
    		      
    		String sql = "CREATE TABLE user_neighborhood " +
    		             "(user_id INTEGER not NULL, " +
    		             "neighbor_id INTEGER, "+
    		             "similarity DOUBLE, "+
    		             " PRIMARY KEY (user_id, neighbor_id))"; 

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
    		}//end finally try
    		}//end try

    	}

	public static void main(String[] args) throws IOException, TasteException {
		Write_neighborhood gen=new Write_neighborhood();
		gen.create_neighborhood_table();
		gen.gen_neighborhood();
	}
}
