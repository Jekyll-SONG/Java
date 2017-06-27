package chapter;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.model.DataModel;

import dao.connection;

public class Chapter_adj {

	public ArrayList<Integer> get_user() throws IOException, TasteException{
        String file = "src/data/testCF.csv";
        DataModel model = new FileDataModel(new File(file));
        LongPrimitiveIterator item=model.getUserIDs();
        ArrayList<Integer> list =new ArrayList<Integer>();
        while(item.hasNext()){
        	String string_id=Long.toString(item.next());
        	int user_id=Integer.parseInt(string_id);
        	list.add(user_id);
        }
        return list;
	}
	
	public void create_view() throws SQLException{
		new dao.connection();
		Connection conn = connection.getDao();
		Statement stmt = null;
		stmt = conn.createStatement();
		
		String del1="drop view if exists view1";
		String del2="drop view if exists view2";
		String del3="drop view if exists view3";
		
		String sql1="create view view1 as select  a.user_id, a.neighbor_id, b.chapter_id, a.similarity, b.isWrong "+
				   "from user_neighborhood as a,  user_chapter_sum as b "+
				   "where a.neighbor_id = b.user_id  and a.method =1 "+
				   "order by user_id, chapter_id, similarity desc";

		String sql2="create view view2 as select distinct user_id, chapter_id, "+
					"sum(isWrong*similarity) as sum_iswrong, sum(similarity)as sum_similarity "+
					"from view1 "+
					"group by user_id, chapter_id";

		String sql3="create view view3 as select user_id, chapter_id, "+
					"(sum_iswrong/sum_similarity) as isWrong "+
					"from view2";
		stmt.executeUpdate(del1);
		stmt.executeUpdate(del2);
		stmt.executeUpdate(del3);
		
		stmt.executeUpdate(sql1);
		stmt.executeUpdate(sql2);
		stmt.executeUpdate(sql3);
		
		conn.close();
	}
	
	public Map<Integer, Double> create_map(int user_id) throws SQLException{
		Map<Integer, Double> user_map=new HashMap<Integer, Double>();
		
		new dao.connection();
		Connection conn = connection.getDao();
		Statement stmt = null;
		stmt = conn.createStatement();
		
		String sql="select chapter_id, isWrong from view3 where user_id="+user_id;
		ResultSet result=stmt.executeQuery(sql);
		
		while(result.next()){
			int chapter_id=result.getInt("chapter_id");
			double isWrong=result.getDouble("isWrong");
			user_map.put(chapter_id, isWrong);
		}
		
		result.close();
		conn.close();
		return user_map;
	}
	
	public Map<Integer, Double> adj_map(Map<Integer, Double> user_map, int user_id) throws SQLException{
		new dao.connection();
		Connection conn = connection.getDao();
		Statement stmt = null;
		stmt = conn.createStatement();
		
		String sql="select chapter_id, isWrong from user_chapter_sum "+
				   "where user_id="+user_id;
		
		ResultSet result=stmt.executeQuery(sql);
		while(result.next()){
			double isWrong=result.getDouble("isWrong");
			int chapter_id=result.getInt("chapter_id");
			user_map.put(chapter_id, isWrong);
		}
		
		result.close();
		conn.close();
		
		System.out.println(user_map.size());
		
		return user_map;
	}
	
	public void delete_table(){
		new dao.connection();
		Connection conn = connection.getDao();
		Statement stmt = null;
		try{
		    stmt = conn.createStatement();
		    String sql = "drop table if exists user_chapter_adj"; 
		    stmt.executeUpdate(sql);
		    System.out.println("Table user_chapter_adj has been dropped");
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
	
	public void create_table(){
		new dao.connection();
		Connection conn = connection.getDao();
		Statement stmt = null;
		try{
		    stmt = conn.createStatement();
		    String sql = "CREATE TABLE user_chapter_adj " +
		                 "(record_id INTEGER not NULL AUTO_INCREMENT, " +
		                 " user_id Integer, "+
		                 " chapter_id Integer, " + 
		                 " isWrong Double, " +
		                 " PRIMARY KEY ( record_id ))"; 
		    stmt.executeUpdate(sql);
		    System.out.println("Table user_chapter_adj has been created");
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
	
	public void write_data(int user_id, Map<Integer, Double> user_map){
		new dao.connection();
		Connection conn = connection.getDao();
		Statement stmt = null;
		String sql="insert into user_chapter_adj (user_id, chapter_id, isWrong) values ";
		
		Set<Integer> list=user_map.keySet();
		Iterator<Integer> iter=list.iterator();
		
		while(iter.hasNext()){
			int chapter_id=iter.next();
			sql+=" ("+user_id+", "+chapter_id+", "+user_map.get(chapter_id)+"),";
		}
		if (sql != null && sql.length() > 0 && sql.charAt(sql.length()-1)==','){
			sql=sql.substring(0, sql.length()-1);
		}

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate(sql);
			conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) throws IOException, TasteException, SQLException{
		Chapter_adj gen=new Chapter_adj();
		gen.delete_table();
		gen.create_table();
//		ArrayList<Integer> item_list=gen.get_item();
		ArrayList<Integer> user_list=gen.get_user();
		
		Map<Integer, Double> user_map=new HashMap<Integer, Double>();
		Map<Integer, Double> user_map_adj=new HashMap<Integer, Double>();
		
		gen.create_view();
		
		for(int i=0; i<user_list.size(); i++){
			user_map=gen.create_map(user_list.get(i));
			user_map_adj=gen.adj_map(user_map, user_list.get(i));
			gen.write_data(user_list.get(i), user_map_adj);
			user_map.clear();
			user_map_adj.clear();
		}
	}
}
