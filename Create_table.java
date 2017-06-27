package neighborhood;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import dao.connection;

public class Create_table {
	
	public void delete(){
		new dao.connection();
		Connection conn = connection.getDao();
		Statement stmt = null;
		try{
			System.out.println("Dropping table user_neighborhood");
		    stmt = conn.createStatement(); 
		    String sql = "drop table if exists user_neighborhood"; 
		    stmt.executeUpdate(sql);
		    System.out.println("Table user_neighborhood has been dropped");
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
	
	public void create(){
    	new dao.connection();
		Connection conn = connection.getDao();
		Statement stmt = null;
		try{
			System.out.println("Creating table user_neighborhood");
		    stmt = conn.createStatement();
		    String sql = "CREATE TABLE user_neighborhood (" +
		                 " user_id Integer, "+
		                 " neighbor_id Integer, " + 
		                 " similarity Double, " +
		                 " method Integer) "; 
		    stmt.executeUpdate(sql);
		    System.out.println("Table user_neighborhood has been created");
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

	public static void main(String[] args){
		Create_table gen=new Create_table();
		gen.delete();
		gen.create();
	}
}
