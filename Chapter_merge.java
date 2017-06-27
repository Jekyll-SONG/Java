package chapter;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood;
import org.apache.mahout.cf.taste.impl.similarity.EuclideanDistanceSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;

import dao.connection;

public class Chapter_merge {
    final static int NEIGHBORHOOD_NUM = 10;
	public List<hibernate.UserKnowledge> get_record_list()
	{
		new dao.connection();
		List<hibernate.UserKnowledge> recordlist = new ArrayList<hibernate.UserKnowledge> ();
		Connection conn = connection.getDao();
		Statement stmt;
		try {
			 stmt = conn.createStatement();
			
			 String sql="select user_id, chapter_id, date, isWrong "+
	    		  		"from user_chapter order by user_id, chapter_id, date";
 	  
			 ResultSet result=stmt.executeQuery(sql);

			 while(result.next())
	    	 {
	    		  Timestamp time=result.getTimestamp("date");
	    		  hibernate.UserKnowledge item = new hibernate.UserKnowledge();
	    		  item.setDate(time);
	    		  item.setIsWrong(result.getInt("isWrong"));
	    		  item.setKnowledgeId(result.getInt("chapter_id"));
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
			}
		}
		
		double pro = calculate_pro_list(wrong_list);
		hibernate.UserKnowledgeWithoutDuplicate item = new hibernate.UserKnowledgeWithoutDuplicate();
		item.setUserId(user_id_pre);
		item.setKnowledgeId(knowledge_id_pre);
		item.setIsWrong(pro);
		recordlist.add(item);

		return recordlist;
	}
	
	public double calculate_pro_list (List<Integer> wrong_list )
	{
		double pro=0;
		for (int i=0; i<wrong_list.size(); i++) 
		{
			pro+=wrong_list.get(i);
		}
		
		return pro;
	}
	
	public boolean save_to_User_knowledge(List<hibernate.UserKnowledgeWithoutDuplicate> myrecordlist)
	{
		String sql="insert into user_chapter_sum (user_id, chapter_id, isWrong) values ";
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
					sql="insert into user_chapter_sum (user_id, chapter_id, isWrong) values ";
					
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		
		return true;
	}
	
	public void delete(){
		new dao.connection();
		Connection conn = connection.getDao();
		Statement stmt = null;
		try{
		    stmt = conn.createStatement();
		    String sql = "drop table if exists user_chapter_sum"; 
		    stmt.executeUpdate(sql);
		 }catch(SQLException se){
		    se.printStackTrace();
		 }catch(Exception e){
		    e.printStackTrace();
		 }finally{
		    try{
		       if(stmt!=null)
		          conn.close();
		    }catch(SQLException se){
		    }
		    try{
		       if(conn!=null)
		          conn.close();
		    }catch(SQLException se){
		       se.printStackTrace();
		    }
		 }
	}
	
	public void create_user_section_sum(){
		new dao.connection();
		
		Connection conn = connection.getDao();
		Statement stmt = null;
		   
		try{
			System.out.println("Creating table in given database...");
		    stmt = conn.createStatement();
		      
		    String sql = "CREATE TABLE user_chapter_sum " +
		                 "(record_id INTEGER not NULL AUTO_INCREMENT, " +
		                 " user_id Integer, "+
		                 " chapter_id Integer, " + 
		                 " isWrong Double, " +
		                 " PRIMARY KEY ( record_id ))"; 

		    stmt.executeUpdate(sql);
		    System.out.println("Created table in given database...");
		 }catch(SQLException se){
		    se.printStackTrace();
		 }catch(Exception e){
		    e.printStackTrace();
		 }finally{
		    try{
		       if(stmt!=null)
		          conn.close();
		    }catch(SQLException se){
		    }
		    try{
		       if(conn!=null)
		          conn.close();
		    }catch(SQLException se){
		       se.printStackTrace();
		    }
		 }
	}
	
	public void gen_neighborhood() throws IOException, TasteException{
		new dao.connection();
		Connection conn = connection.getDao();
		Statement stmt = null;
		
	    String file = "src/data/testCF.csv";
	    DataModel model = new FileDataModel(new File(file));
	    UserSimilarity user = new EuclideanDistanceSimilarity(model);
	    NearestNUserNeighborhood neighbor = new NearestNUserNeighborhood(NEIGHBORHOOD_NUM, user, model);

		try{
			stmt = conn.createStatement();
			LongPrimitiveIterator iter=model.getUserIDs();

			while(iter.hasNext()){
				String sql="insert into user_neighborhood (user_id, neighbor_id, similarity, method) values";
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
			      se.printStackTrace();
			   }catch(Exception e){
			      e.printStackTrace();
			   }finally{
			      try{
			         if(stmt!=null)
			            conn.close();
			      }catch(SQLException se){
			      }
			      try{
			         if(conn!=null)
			            conn.close();
			      }catch(SQLException se){
			         se.printStackTrace();
			      }
			   }
	    }

	public static void main(String[] args) {
		Chapter_merge gen = new Chapter_merge();
		gen.delete();
		gen.create_user_section_sum();
		
		List<hibernate.UserKnowledge> recordlist=gen.get_record_list();
		List<hibernate.UserKnowledgeWithoutDuplicate> recordlist_witho_dup =gen.calculate_prob(recordlist);
		
		gen.save_to_User_knowledge(recordlist_witho_dup);
		System.out.println("success");
	}
}
