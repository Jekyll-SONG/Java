package chapter;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import org.hibernate.Transaction;

import dao.connection;

public class User_chapter {
	
	public List<hibernate.UserKnowledge> get_record_list()
	{
		new dao.connection();
		List<hibernate.UserKnowledge> recordlist = new ArrayList<hibernate.UserKnowledge> ();
		
		Connection conn = connection.getDao();
		Statement stmt;
		try {
			 stmt = conn.createStatement();
			
			 String sql=" select U.user_id as user_id, B.chapter_id as chapter_id, U.date as date, U.isWrong as isWrong"+
	    		  		" from user_section U, books_sections B"+
	    		  		" where U.section_id=B.id"+
	    		  		" order by user_id, chapter_id";
 	  
			 ResultSet result=stmt.executeQuery(sql);
			 System.out.println("Getting the record");

			 while(result.next()){
	    		  Timestamp time=result.getTimestamp("date");
	    		  
	    		  hibernate.UserKnowledge item = new hibernate.UserKnowledge();
	    		  item.setDate(time);
	    		  item.setIsWrong(result.getInt("isWrong"));
	    		  item.setKnowledgeId(result.getInt("chapter_id"));
	    		  item.setUserId(result.getInt("user_id"));
	    		  recordlist.add(item);
	    	  }
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		 try {
			conn.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}  
		System.out.println("Get record success, size: "+recordlist.size());
		return recordlist;
	}
	
	public boolean write_to_User_knowledge(List<hibernate.UserKnowledge> myrecordlist)
	{
		String sql="insert into user_chapter (user_id, chapter_id, date, isWrong) values ";
		// 533925 
		for (int i=0; i<myrecordlist.size();i++)
		{
			hibernate.UserKnowledge record = myrecordlist.get(i);

			sql+=" ("+record.getUserId()+", "+record.getKnowledgeId()+", '"+record.getDate()+"', "+record.getIsWrong()+"),";
			if(i==myrecordlist.size()-1 || i%999==0) 
			{
				sql=sql.substring(0, sql.length()-1);
				new dao.connection();
				Connection conn = connection.getDao();
				Statement stmt;
				try {
					stmt = conn.createStatement();
					stmt.executeUpdate(sql);
					conn.close();
					sql="insert into user_chapter (user_id, chapter_id, date, isWrong) values ";
					
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
		}
		
		return true;
	}
	
	public int AddRecord(hibernate.UserKnowledge event)
	{   
		// -2 is not in topic.
		 int event_id = -2;
  		 // 2. Create DAO
		 hibernate.UserKnowledgeDAO dao = new hibernate.UserKnowledgeDAO();
		 // 3. Start the transaction
		 Transaction tx = dao.getSession().beginTransaction();
		 // 4. Add user
		 dao.save(event);
		 event_id=event.getRecordId();
		 // 5. Commit the transaction (write to database)
		 tx.commit();
		 // 6. Close the session (cleanup connections)
		 dao.getSession().close();
		 return event_id;
	}
	
	public void create_user_chapter_table(){
		new dao.connection();
		Connection conn = connection.getDao();
		Statement stmt = null;

		try{
		    System.out.println("Connecting to a selected database...");
		    System.out.println("Connected database successfully...");
		      
		    //STEP 4: Execute a query
		    System.out.println("Creating table in given database...");
		    stmt = conn.createStatement();
		      
		    String sql = "CREATE TABLE user_chapter " +
		                 "(record_id INTEGER not NULL AUTO_INCREMENT, " +
		                 " user_id Integer, "+
		                 " chapter_id Integer, " + 
		                 " date timestamp, " + 
		                 " isWrong Integer, " +
		                 " PRIMARY KEY ( record_id ))"; 

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
		 }
	}
	
	public void delete(){
		new dao.connection();
		Connection conn = connection.getDao();
		Statement stmt = null;

		try{
		    System.out.println("Connecting to a selected database...");
		    System.out.println("Connected database successfully...");
		      
		    //STEP 4: Execute a query
		    System.out.println("Creating table in given database...");
		    stmt = conn.createStatement();
		      
		    String sql = "drop table if exists user_chapter"; 

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
		 }
	}

	public static void main(String[] args) {
		User_chapter gen = new User_chapter();
		gen.delete();
		gen.create_user_chapter_table();
		List<hibernate.UserKnowledge> recordlist  = gen.get_record_list();
		gen.write_to_User_knowledge(recordlist);
		
		System.out.println("success");
	}//end main

}
