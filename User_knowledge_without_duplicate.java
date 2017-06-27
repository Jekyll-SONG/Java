package general;

import java.sql.*;
import java.util.*;

import dao.connection;

public class User_knowledge_without_duplicate {

	public void save_to_user_know_without_depulicate()
	{
		Connection conn = null;
		Statement stmt = null;
		try{
		 
			new dao.connection();
			conn = connection.getDao();
			//STEP 4: Execute a query
			 
			stmt = conn.createStatement();
	      
			//get the collection of the user_id
			String sql_id="select distinct user_id from user_knowledge order by user_id";
			ResultSet rs_id=stmt.executeQuery(sql_id);

			List<Integer> listUser=new ArrayList<Integer>();   
			try {
				while(rs_id.next()){
					listUser.add(rs_id.getInt("user_id"));
				} 
			}catch (SQLException e){
				e.printStackTrace();
			}
	      
			System.out.println(listUser.size());
	      
			//get the collection of the knowledge_id
			String sql_knowledge="select distinct knowledge_id from user_knowledge order by knowledge_id";
	        ResultSet rs_knowledge=stmt.executeQuery(sql_knowledge);
  	    
	        List<Integer> listKnowledge=new ArrayList<Integer>();   

	        try {
	    	    while(rs_knowledge.next()){
	    		    listKnowledge.add(rs_knowledge.getInt("knowledge_id"));
	      	    } 
	        }catch (SQLException e){
	    	    e.printStackTrace();
	        }
	      
	        System.out.println(listKnowledge.size());

	        int record_id=1;
	      
	        for(int i=0; i<listUser.size()-1; i++){
	        	String sql_insert="insert into user_knowledge_without_duplicate (record_id, user_id, knowledge_id, isWrong) values ";
	    	    for(int j=0; j<listKnowledge.size(); j++){
	    		    String sql = "SELECT isWrong FROM user_knowledge "+
	    			    			" where user_id= "+listUser.get(i)+" and knowledge_id = "+listKnowledge.get(j);
	    		    ResultSet rs = stmt.executeQuery(sql);
	    	  
	    		    List<Integer> isWrong=new ArrayList<Integer>();
	    	  
	    		    while(rs.next()){
	    			    isWrong.add(rs.getInt("isWrong"));
	    	  	    }
	    	  
	    		    double sum=0.0;
	    		    for(int m=0; m<isWrong.size(); m++){
	    			    sum+=isWrong.get(m);
	    		    }
	    		    double avg_isWrong=(isWrong.size()==0? 0:(sum/isWrong.size()) );
	    	  
	    		    sql_insert+=" ("+ record_id+", "+listUser.get(i)+", "+listKnowledge.get(j)+", "+avg_isWrong+" ),";

	    		    record_id++;
	    	    }
	    	    
	    	    //eliminate the comma at the end of the string
	    	    if (sql_insert != null && sql_insert.length() > 0 && sql_insert.charAt(sql_insert.length()-1)==',') {
	    	    	sql_insert = sql_insert.substring(0, sql_insert.length()-1);
	    	    }

    		    stmt.executeUpdate(sql_insert);
	        }
	      

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
		
	     System.out.println("Goodbye!");
	}
	
	public static void main(String[] args) {
	
	}//end main
	
}
