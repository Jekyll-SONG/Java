package com.xie.behavior;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood;
import org.apache.mahout.cf.taste.impl.similarity.EuclideanDistanceSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;

import dao.connection;
/***
 * 
 * write into db from CSV results.
 * @author Li Tao
 *
 */
public class Write_behavior_neighborhood {
    final static int NEIGHBORHOOD_NUM = 10;
    
    
    public void drop_table()
    
    {Connection conn = new dao.connection().getDao();
	Statement stmt = null;
	   
	try{
 	    stmt = conn.createStatement();
	      
	    String sql = "drop table if exists user_neighborhood";

	    stmt.executeUpdate(sql);
	    System.out.println("drop table success...");
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
    public void create_table(){
    	drop_table();
    	Connection conn = new dao.connection().getDao();
		Statement stmt = null;
		   
		try{
			System.out.println("Creating table in given database...");
		    stmt = conn.createStatement();
		      
		    String sql = " CREATE TABLE user_neighborhood (" +
		                 " user_id Integer, "+
		                 " neighbor_id Integer, " + 
		                 " similarity Double, " +
		                 " method Integer"
		            //     + ",  PRIMARY KEY ( user_id, neighbor_id )"
		                 + ")"; 

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
    
    public void write_neighborhood() throws IOException, TasteException{
		new dao.connection();
		Connection conn = connection.getDao();
		Statement stmt = null;
		
        String file = "src/data/testCF.csv";
        
        //when duplicate, this function uses override method
        DataModel model = new FileDataModel(new File(file));
        
        UserSimilarity user = new EuclideanDistanceSimilarity(model);
        
        //create the 10 nearest neighbors
        NearestNUserNeighborhood neighbor = new NearestNUserNeighborhood(NEIGHBORHOOD_NUM, user, model);
        
		try{
		      System.out.println("Creating table in given database...");
		      stmt = conn.createStatement();
		      
		      LongPrimitiveIterator iter=model.getUserIDs();
     
		      while(iter.hasNext()){
			      String sql="insert into user_neighborhood (user_id, neighbor_id, similarity, method) values";
		    	  
			      for(int m=0; m<50&&iter.hasNext(); m++){
			    	  String string_id=Long.toString(iter.next());
			    	  int user_id=Integer.parseInt(string_id);
			      
			    	  long neighbor_id[]=neighbor.getUserNeighborhood(user_id);
			      

			    	  for(int j=0; j<NEIGHBORHOOD_NUM; j++){
			    		  sql+=" ("+user_id+", "+neighbor_id[j]+", "+user.userSimilarity(user_id, neighbor_id[j])+", 1),";
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
    
    public static void main(String[] args) throws IOException, TasteException {
    	
    	Write_behavior_neighborhood item=new Write_behavior_neighborhood();
    	item.create_table();
    	item.write_neighborhood();
	}
}
