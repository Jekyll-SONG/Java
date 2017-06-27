package com.xie.googleclick;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import dao.connection;

public class Eliminate_dup_svg {
	public List<hibernate.UserKnowledge> get_record_list()
	{
		new dao.connection();
		List<hibernate.UserKnowledge> recordlist = new ArrayList<hibernate.UserKnowledge> ();
		
		Connection conn = connection.getDao();
		Statement stmt;
		try {
			 stmt = conn.createStatement();
			
			 String sql="select user_id, knowledge_id, date, isWrong "+
	    		  		"from user_knowledge order by user_id,knowledge_id, date";
 	  
			 ResultSet result=stmt.executeQuery(sql);

			 while(result.next())
	    	 {
	    		  Timestamp time=result.getTimestamp("date");
	    		  hibernate.UserKnowledge item = new hibernate.UserKnowledge();
	    		  item.setDate(time);
	    		  item.setIsWrong(result.getInt("isWrong"));
	    		  item.setKnowledgeId(result.getInt("knowledge_id"));
	    		  item.setUserId(result.getInt("user_id"));
	    		  recordlist.add(item);
	    	 }
		} catch (SQLException e) {
			e.printStackTrace();
		}
		 try {
			conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}  

		System.out.println("Success, recordlist.size£º"+recordlist.size());
		
		return recordlist;
	}
	
	public List<hibernate.UserKnowledgeWithoutDuplicate> calculate_prob(List<hibernate.UserKnowledge> myrecordlist)
	{
		List<hibernate.UserKnowledgeWithoutDuplicate> recordlist = new ArrayList<hibernate.UserKnowledgeWithoutDuplicate> ();
		
		hibernate.UserKnowledge myrecord_pre = new hibernate.UserKnowledge();
		myrecord_pre = myrecordlist.get(0);
		Integer user_id_pre = myrecord_pre.getUserId();
		Integer knowledge_id_pre = myrecord_pre.getKnowledgeId();
		List<Integer> wrong_list = new ArrayList<Integer>();
		wrong_list.add(myrecord_pre.getIsWrong());
		int size=myrecordlist.size();

		for(int num=1; num<size; num++)
		{
			hibernate.UserKnowledge myrecord = myrecordlist.get(num);
			int user_id_now=myrecord.getUserId();
			int knowledge_id_now=myrecord.getKnowledgeId();
			
			if( user_id_now==user_id_pre && knowledge_id_now==knowledge_id_pre)  
			{
				wrong_list.add(myrecord.getIsWrong());
			}
			else
			{
				double pro = calculate_pro_list(wrong_list);
				hibernate.UserKnowledgeWithoutDuplicate item = new hibernate.UserKnowledgeWithoutDuplicate();
				item.setUserId(user_id_pre);
				item.setKnowledgeId(knowledge_id_pre);
				item.setIsWrong(pro);
				recordlist.add(item);
				wrong_list.clear();
				wrong_list.add(myrecord.getIsWrong());
				
				user_id_pre = myrecord.getUserId() ;
				knowledge_id_pre = myrecord.getKnowledgeId() ;
				
//				System.out.println(user_id_pre+" "+knowledge_id_pre);
			}
		}
		
		double pro = calculate_pro_list(wrong_list);
		hibernate.UserKnowledgeWithoutDuplicate item = new hibernate.UserKnowledgeWithoutDuplicate();
		item.setUserId(myrecord_pre.getUserId());
		item.setKnowledgeId(myrecord_pre.getKnowledgeId());
		item.setIsWrong(pro);
		recordlist.add(item);

//		System.out.println("Success, recordlist: "+recordlist.size());
		return recordlist;
	}
	
	public double calculate_pro_list (List<Integer> wrong_list )
	{
		double pro=0;
		for (int i=0; i<wrong_list.size(); i++) 
		{
			pro+=wrong_list.get(i);
		}
		
		return pro/wrong_list.size();
	}
	
	public boolean save_to_User_knowledge(List<hibernate.UserKnowledgeWithoutDuplicate> myrecordlist)
	{
		String sql="insert into user_knowledge_svg (user_id, knowledge_id, isWrong) values ";
		// 533925 
		for (int i=0; i<myrecordlist.size();i++)
		{
			hibernate.UserKnowledgeWithoutDuplicate record = myrecordlist.get(i);

			sql+=" ("+record.getUserId()+", "+record.getKnowledgeId()+", "+record.getIsWrong()+"),";
			if(i==myrecordlist.size()-1 || i%500 ==0 ) 
			{
				if (sql != null && sql.length() > 0 && sql.charAt(sql.length()-1)==','){
					sql=sql.substring(0, sql.length()-1);
				}
				
				new dao.connection();
				Connection conn = connection.getDao();
				Statement stmt;
				try {
					stmt = conn.createStatement();
					stmt.executeUpdate(sql);
					conn.close();
//					
//					System.out.println("--------" + i);
					sql="insert into user_knowledge_svg (user_id, knowledge_id, isWrong) values ";
					
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		
		return true;
	}
	
	public void create_user_knowledge_avg(){
		new dao.connection();
		
		Connection conn = connection.getDao();
		Statement stmt = null;
		   
		try{
			System.out.println("Creating table in given database...");
		    stmt = conn.createStatement();
		      
		    String sql = "CREATE TABLE user_knowledge_svg " +
		                 "(record_id INTEGER not NULL AUTO_INCREMENT, " +
		                 " user_id Integer, "+
		                 " knowledge_id Integer, " + 
		                 " isWrong Double, " +
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
		    }
		 }
	}
	
	public static void main(String[] args) {
		Eliminate_dup_svg gen = new Eliminate_dup_svg();
		gen.create_user_knowledge_avg();
		
		List<hibernate.UserKnowledge> recordlist=gen.get_record_list();
		List<hibernate.UserKnowledgeWithoutDuplicate> recordlist_witho_dup =gen.calculate_prob(recordlist);
		
		gen.save_to_User_knowledge(recordlist_witho_dup);
		System.out.println("success");
	}
}
