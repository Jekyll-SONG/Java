package chapter;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
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

public class Chapter_merge_neighbor {
	final int NEIGHBORHOOD_NUM=10;
	

	public void clear_data() throws IOException{
		File csv = new File("src/data/testCF.csv");
		FileWriter fw =  new FileWriter(csv);
		fw.write("");
		fw.close();
		System.out.println("Clear finished");
	}
	
	public void write_data(){
		
		new dao.connection();
		Connection conn = connection.getDao();
		Statement stmt = null;
		try{
			stmt = conn.createStatement();

			List<Integer> user_id=new ArrayList<Integer>(); 
			List<Integer> knowledge_id=new ArrayList<Integer>(); 
			List<Double> isWrong=new ArrayList<Double>(); 

			String sql = "select user_id, chapter_id, isWrong from user_chapter_sum";
			ResultSet rs = stmt.executeQuery(sql);

			while(rs.next()){
				user_id.add(rs.getInt("user_id"));
				knowledge_id.add(rs.getInt("chapter_id"));
				isWrong.add(rs.getDouble("isWrong"));
			}
	    
			rs.close();
			
			int size=user_id.size();

			File csv = new File("src/data/testCF.csv");

			BufferedWriter bw = new BufferedWriter(new FileWriter(csv, true));
			for(int i=0; i<size; i++){
				bw.write(user_id.get(i) + "," +knowledge_id.get(i)+ "," + isWrong.get(i)); 
				bw.newLine(); 
			}
	    	
			bw.close();
      
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

	public void write_neighborhood() throws IOException, TasteException{
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
			    	  
			    	  System.out.println(user_id);
			      
			    	  long neighbor_id[]=neighbor.getUserNeighborhood(user_id);

			    	  for(int j=0; j<NEIGHBORHOOD_NUM && j<neighbor_id.length; j++){
			    		  sql+=" ("+user_id+", "+neighbor_id[j]+", "+user.userSimilarity(user_id, neighbor_id[j])+", 4),";
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
		Chapter_merge_neighbor gen=new Chapter_merge_neighbor();
		try {
			gen.clear_data();
		} catch (IOException e) {

			e.printStackTrace();
		}
		gen.write_data();
		
		try {
			gen.write_neighborhood();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (TasteException e) {
			e.printStackTrace();
		}

	}
}
