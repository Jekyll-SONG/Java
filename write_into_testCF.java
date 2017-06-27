package com.xie.googleclick;

//STEP 1. Import required packages
import java.sql.*;
import java.io.BufferedWriter; 
import java.io.File; 
//import java.io.FileNotFoundException; 
import java.io.FileWriter; 
import java.io.IOException;
//import java.io.IOException; 
import java.util.*;  

import dao.connection;
/***
 * 
 * 
 * Generate CSV for Calculate.
 * @author Li Tao
 *
 */
public class write_into_testCF {

	public void clear_data() throws IOException{
			//connect to file
			File csv = new File("src/data/testCF.csv");
			
			//create the file writer
			FileWriter fw =  new FileWriter(csv);
			
			//delete all existing data in testCF file
			fw.write("");
			
			//close the file
			fw.close();
			
			//inform the user
			System.out.println("Clear finished");
	}

	public void write_data(){
		
		new dao.connection();
		Connection conn = connection.getDao();
		Statement stmt = null;
    	try{
    	        
    	    //STEP 4: Execute a query
    	    System.out.println("Creating statement...");
    	    stmt = conn.createStatement();

    		List<Integer> user_id=new ArrayList<Integer>(); 
    		List<Integer> knowledge_id=new ArrayList<Integer>(); 
    		List<Double> isWrong=new ArrayList<Double>(); 

    	    String sql = "select user_id, knowledge_id, isWrong from user_knowledge_svg";
    	    ResultSet rs = stmt.executeQuery(sql);

    	    while(rs.next()){
    	    	user_id.add(rs.getInt("user_id"));
    	    	knowledge_id.add(rs.getInt("knowledge_id"));
    	    	isWrong.add(rs.getDouble("isWrong"));
    	    }
    	    
    	    rs.close();
    	    
    	    int size=user_id.size();

    	    File csv = new File("src/data/testCF.csv"); // csv file
    
    	    BufferedWriter bw = new BufferedWriter(new FileWriter(csv, true)); // add to new line
    	    for(int i=0; i<size; i++){
    	    	bw.write(user_id.get(i) + "," +knowledge_id.get(i)+ "," + isWrong.get(i)); 
    	    	bw.newLine(); 
    	    }
    	    	
    	    bw.close();
          
 	      }catch(SQLException se){
	        //Handle errors for JDBC
	        se.printStackTrace();
	      }catch(Exception e){
	        //Handle errors for Class.forName
	        e.printStackTrace();
//	      }catch (FileNotFoundException e) { 
//          // File object exception catch
//          e.printStackTrace(); 
//          }catch (IOException e) { 
//          // BufferedWriter close exception catch
//          e.printStackTrace(); 
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

	public static void main(String[] args) { 
		write_into_testCF gen=new write_into_testCF();
		try {
			gen.clear_data();
		} catch (IOException e) {

			e.printStackTrace();
		}
		gen.write_data();
	}
}
	
