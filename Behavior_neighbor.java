package behavior;

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

public class Behavior_neighbor {
    final static int NEIGHBORHOOD_NUM = 10;
	
	public void clear_data() throws IOException{
		File csv = new File("src/data/testCF.csv");
		FileWriter fw =  new FileWriter(csv);
		fw.write("");
		fw.close();
		System.out.println("File has been cleared");
	}

	public double avg(ArrayList<Double> list){
		double sum=0;
		for(int i=0; i<list.size(); i++){
			sum+=list.get(i);
		}
		double average=sum/list.size();
		return average;
	}
	
	public double std(ArrayList<Double> list){
		double mean=avg(list);
		double diff=0;
		for(int i=0; i<list.size(); i++){
			diff+=Math.pow((list.get(i)-mean),2);
		}
		double standard_deviation=0;
		if(diff<=0){
			return 0;
		}
		else{
			standard_deviation=Math.pow(diff/list.size(), 0.5);
		}
		return standard_deviation;
	}
	
	public ArrayList<Double> standardise_list(ArrayList<Double> list){
		ArrayList<Double> std_list=new ArrayList<Double>();
		for(int i=0; i<list.size(); i++){
			double data=(list.get(i)-avg(list))/std(list);
			std_list.add(data);
		}
		return std_list;
	}
	
	public void write_data(){	
		new dao.connection();
		Connection conn = connection.getDao();
		Statement stmt = null;
		List<Integer> user=new ArrayList<Integer>(); 
		
		//in total 36 items
		ArrayList<Double> during=new ArrayList<Double>();
		ArrayList<Double> during_experiment=new ArrayList<Double>();
		ArrayList<Double> click_total=new ArrayList<Double>();
		ArrayList<Double> action_during_sum=new ArrayList<Double>();
		ArrayList<Double> action_during_avg=new ArrayList<Double>();
		ArrayList<Double> action_leave_sum=new ArrayList<Double>();
		ArrayList<Double> action_quit_sum=new ArrayList<Double>();
		ArrayList<Double> action_leave_problem_sum=new ArrayList<Double>();
		ArrayList<Double> action_quit_problem_sum=new ArrayList<Double>();
		ArrayList<Double> active_day=new ArrayList<Double>();
		ArrayList<Double> active_time_day=new ArrayList<Double>();
		ArrayList<Double> login_days_total=new ArrayList<Double>();
		ArrayList<Double> usage_daily_avg=new ArrayList<Double>();
		ArrayList<Double> usage_daily_std=new ArrayList<Double>();
		ArrayList<Double> login_prob=new ArrayList<Double>();
		ArrayList<Double> click_prob=new ArrayList<Double>();
		ArrayList<Double> click_once_login=new ArrayList<Double>();
		ArrayList<Double> usage_time_prob=new ArrayList<Double>();
		ArrayList<Double> usage_time_once_login=new ArrayList<Double>();
		ArrayList<Double> action_leave_prob=new ArrayList<Double>();
		ArrayList<Double> login_prob_2=new ArrayList<Double>();
		ArrayList<Double> action_leave_problem_prob=new ArrayList<Double>();
		ArrayList<Double> active_click_prob=new ArrayList<Double>();
		ArrayList<Double> active_time_prob=new ArrayList<Double>();
		ArrayList<Double> action_leave_question_prob_perc=new ArrayList<Double>();
		ArrayList<Double> action_quit_question_prob_perc=new ArrayList<Double>();
		ArrayList<Double> come_back=new ArrayList<Double>();
		ArrayList<Double> come_back2=new ArrayList<Double>();
		ArrayList<Double> come_back3=new ArrayList<Double>();
		ArrayList<Double> active_user_click_prob=new ArrayList<Double>();
		ArrayList<Double> active_user_login_prob=new ArrayList<Double>();
		ArrayList<Double> active_user_usage_time_prob=new ArrayList<Double>();
		ArrayList<Double> active_user_login_prob_2=new ArrayList<Double>();
		ArrayList<Double> active_user_active_click_prob=new ArrayList<Double>();
		ArrayList<Double> active_user_active_time_prob=new ArrayList<Double>();
		ArrayList<Double> active_user_click_total=new ArrayList<Double>();

    	try{
    	    stmt = conn.createStatement();

    	    String sql = "select * from table_1";
    	    ResultSet rs = stmt.executeQuery(sql);
    	    
    	    while(rs.next()){
    	    	user.add(rs.getInt("user_id"));
									
    	    	during.add(rs.getDouble("during"));
    	    	during_experiment.add(rs.getDouble("during_experiment"));
    	    	click_total.add(rs.getDouble("click_total"));
    	    	action_during_sum.add(rs.getDouble("action_during_sum"));
    	    	action_during_avg.add(rs.getDouble("action_during_avg"));
    	    	action_leave_sum.add(rs.getDouble("action_leave_sum"));
    	    	action_quit_sum.add(rs.getDouble("action_quit_sum"));
    	    	action_leave_problem_sum.add(rs.getDouble("action_leave_problem_sum"));
    	    	action_quit_problem_sum.add(rs.getDouble("action_quit_problem_sum"));
    	    	active_day.add(rs.getDouble("active_day"));
    	    	active_time_day.add(rs.getDouble("active_time_day"));
    	    	login_days_total.add(rs.getDouble("login_days_total"));
    	    	usage_daily_avg.add(rs.getDouble("usage_daily_avg"));
    	    	usage_daily_std.add(rs.getDouble("usage_daily_std"));
    	    	login_prob.add(rs.getDouble("login_prob"));
    	    	click_prob.add(rs.getDouble("click_prob"));
    	    	click_once_login.add(rs.getDouble("click_once_login"));
    	    	usage_time_prob.add(rs.getDouble("usage_time_prob"));
    	    	usage_time_once_login.add(rs.getDouble("usage_time_once_login"));
    	    	action_leave_prob.add(rs.getDouble("action_leave_prob"));
    	    	login_prob_2.add(rs.getDouble("login_prob_2"));
    	    	action_leave_problem_prob.add(rs.getDouble("action_leave_problem_prob"));
    	    	active_click_prob.add(rs.getDouble("active_click_prob"));
    	    	active_time_prob.add(rs.getDouble("active_time_prob"));
    	    	action_leave_question_prob_perc.add(rs.getDouble("action_leave_question_prob_perc"));
    	    	action_quit_question_prob_perc.add(rs.getDouble("action_quit_question_prob_perc"));
    	    	come_back.add(rs.getDouble("come_back"));
    	    	come_back2.add(rs.getDouble("come_back2"));
    	    	come_back3.add(rs.getDouble("come_back3"));
    	    	active_user_click_prob.add(rs.getDouble("active_user_click_prob"));
    	    	active_user_login_prob.add(rs.getDouble("active_user_login_prob"));
    	    	active_user_usage_time_prob.add(rs.getDouble("active_user_usage_time_prob"));
    	    	active_user_login_prob_2.add(rs.getDouble("active_user_login_prob_2"));
    	    	active_user_active_click_prob.add(rs.getDouble("active_user_active_click_prob"));
    	    	active_user_active_time_prob.add(rs.getDouble("active_user_active_time_prob"));
    	    	active_user_click_total.add(rs.getDouble("active_user_click_total"));
    	    }
    	    
    	    rs.close();
    	    
    	    int size=user.size();

    	    File csv = new File("src/data/testCF.csv");
    
    	    BufferedWriter bw = new BufferedWriter(new FileWriter(csv, true));
    	    
    	    ArrayList<Double> during_std=standardise_list(during);
    	    ArrayList<Double> during_experiment_std=standardise_list(during_experiment);
    	    ArrayList<Double> click_total_std=standardise_list(click_total);
    	    ArrayList<Double> action_during_sum_std=standardise_list(action_during_sum);
    	    ArrayList<Double> action_during_avg_std=standardise_list(action_during_avg);
    	    ArrayList<Double> action_leave_sum_std=standardise_list(action_leave_sum);
    	    ArrayList<Double> action_quit_sum_std=standardise_list(action_quit_sum);
    	    ArrayList<Double> action_leave_problem_sum_std=standardise_list(action_leave_problem_sum);
    	    ArrayList<Double> action_quit_problem_sum_std=standardise_list(action_quit_problem_sum);
    	    ArrayList<Double> active_day_std=standardise_list(active_day);
    	    ArrayList<Double> active_time_day_std=standardise_list(active_time_day);
    	    ArrayList<Double> login_days_total_std=standardise_list(login_days_total);
    	    ArrayList<Double> usage_daily_avg_std=standardise_list(usage_daily_avg);
    	    ArrayList<Double> usage_daily_std_std=standardise_list(usage_daily_std);
    	    ArrayList<Double> login_prob_std=standardise_list(login_prob);
    	    ArrayList<Double> click_prob_std=standardise_list(during);standardise_list(click_prob);
    	    ArrayList<Double> click_once_login_std=standardise_list(click_once_login);
    	    ArrayList<Double> usage_time_prob_std=standardise_list(usage_time_prob);
    	    ArrayList<Double> usage_time_once_login_std=standardise_list(usage_time_once_login);
    	    ArrayList<Double> action_leave_prob_std=standardise_list(action_leave_prob);
    	    ArrayList<Double> login_prob_2_std=standardise_list(login_prob_2);
    	    ArrayList<Double> action_leave_problem_prob_std=standardise_list(action_leave_problem_prob);
    	    ArrayList<Double> active_click_prob_std=standardise_list(active_click_prob);
    	    ArrayList<Double> active_time_prob_std=standardise_list(active_time_prob);
    	    ArrayList<Double> action_leave_question_prob_perc_std=standardise_list(action_leave_question_prob_perc);
    	    ArrayList<Double> action_quit_question_prob_perc_std=standardise_list(action_quit_question_prob_perc);
    	    ArrayList<Double> come_back_std=standardise_list(come_back);
    	    ArrayList<Double> come_back2_std=standardise_list(come_back2);
    	    ArrayList<Double> come_back3_std=standardise_list(come_back3);
    	    ArrayList<Double> active_user_click_prob_std=standardise_list(active_user_click_prob);
    	    ArrayList<Double> active_user_login_prob_std=standardise_list(active_user_login_prob);
    	    ArrayList<Double> active_user_usage_time_prob_std=standardise_list(active_user_usage_time_prob);
    	    ArrayList<Double> active_user_login_prob_2_std=standardise_list(active_user_login_prob_2);
    	    ArrayList<Double> active_user_active_click_prob_std=standardise_list(active_user_active_click_prob);
    	    ArrayList<Double> active_user_active_time_prob_std=standardise_list(active_user_active_time_prob);
    	    ArrayList<Double> active_user_click_total_std=standardise_list(active_user_click_total);

    	    for(int i=0; i<size; i++){
    	    	bw.write(user.get(i)+","+1+","+during_std.get(i)); 
    	    	bw.newLine(); 
    	    	bw.write(user.get(i)+","+2+","+during_experiment_std.get(i)); 
    	    	bw.newLine(); 
    	    	bw.write(user.get(i)+","+3+","+click_total_std.get(i)); 
    	    	bw.newLine(); 
    	    	bw.write(user.get(i)+","+4+","+action_during_sum_std.get(i)); 
    	    	bw.newLine(); 
    	    	bw.write(user.get(i)+","+5+","+action_during_avg_std.get(i)); 
    	    	bw.newLine(); 
    	    	bw.write(user.get(i)+","+6+","+action_leave_sum_std.get(i)); 
    	    	bw.newLine(); 
    	    	bw.write(user.get(i)+","+7+","+action_quit_sum_std.get(i)); 
    	    	bw.newLine(); 
    	    	bw.write(user.get(i)+","+8+","+action_leave_problem_sum_std.get(i)); 
    	    	bw.newLine(); 
    	    	bw.write(user.get(i)+","+9+","+action_quit_problem_sum_std.get(i)); 
    	    	bw.newLine(); 
    	    	bw.write(user.get(i)+","+10+","+active_day_std.get(i));
    	    	bw.newLine(); 
    	    	bw.write(user.get(i)+","+11+","+active_time_day_std.get(i)); 
    	    	bw.newLine(); 
    	    	bw.write(user.get(i)+","+12+","+login_days_total_std.get(i)); 
    	    	bw.newLine(); 
    	    	bw.write(user.get(i)+","+13+","+usage_daily_avg_std.get(i)); 
    	    	bw.newLine(); 
    	    	bw.write(user.get(i)+","+14+","+usage_daily_std_std.get(i)); 
    	    	bw.newLine(); 
    	    	bw.write(user.get(i)+","+15+","+login_prob_std.get(i)); 
    	    	bw.newLine(); 
    	    	bw.write(user.get(i)+","+16+","+click_prob_std.get(i)); 
    	    	bw.newLine(); 
    	    	bw.write(user.get(i)+","+17+","+click_once_login_std.get(i)); 
    	    	bw.newLine(); 
    	    	bw.write(user.get(i)+","+18+","+usage_time_prob_std.get(i)); 
    	    	bw.newLine(); 
    	    	bw.write(user.get(i)+","+19+","+usage_time_once_login_std.get(i)); 
    	    	bw.newLine(); 
    	    	bw.write(user.get(i)+","+20+","+action_leave_prob_std.get(i)); 
    	    	bw.newLine(); 
    	    	bw.write(user.get(i)+","+21+","+login_prob_2_std.get(i)); 
    	    	bw.newLine(); 
    	    	bw.write(user.get(i)+","+22+","+action_leave_problem_prob_std.get(i)); 
    	    	bw.newLine(); 
    	    	bw.write(user.get(i)+","+23+","+active_click_prob_std.get(i)); 
    	    	bw.newLine(); 
    	    	bw.write(user.get(i)+","+24+","+active_time_prob_std.get(i)); 
    	    	bw.newLine(); 
    	    	bw.write(user.get(i)+","+25+","+action_leave_question_prob_perc_std.get(i)); 
    	    	bw.newLine(); 
    	    	bw.write(user.get(i)+","+26+","+action_quit_question_prob_perc_std.get(i)); 
    	    	bw.newLine(); 
    	    	bw.write(user.get(i)+","+27+","+come_back_std.get(i)); 
    	    	bw.newLine(); 
    	    	bw.write(user.get(i)+","+28+","+come_back2_std.get(i)); 
    	    	bw.newLine(); 
    	    	bw.write(user.get(i)+","+29+","+come_back3_std.get(i)); 
    	    	bw.newLine(); 
    	    	bw.write(user.get(i)+","+30+","+active_user_click_prob_std.get(i));
    	    	bw.newLine(); 
    	    	bw.write(user.get(i)+","+31+","+active_user_login_prob_std.get(i)); 
    	    	bw.newLine(); 
    	    	bw.write(user.get(i)+","+32+","+active_user_usage_time_prob_std.get(i)); 
    	    	bw.newLine(); 
    	    	bw.write(user.get(i)+","+33+","+active_user_login_prob_2_std.get(i)); 
    	    	bw.newLine(); 
    	    	bw.write(user.get(i)+","+34+","+active_user_active_click_prob_std.get(i)); 
    	    	bw.newLine(); 
    	    	bw.write(user.get(i)+","+35+","+active_user_active_time_prob_std.get(i)); 
    	    	bw.newLine(); 
    	    	bw.write(user.get(i)+","+36+","+active_user_click_total_std.get(i)); 
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
  	     System.out.println("Data has been written to the file");
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
			      
			    	  long neighbor_id[]=neighbor.getUserNeighborhood(user_id);

			    	  for(int j=0; j<NEIGHBORHOOD_NUM && j<neighbor_id.length; j++){
			    		  sql+=" ("+user_id+", "+neighbor_id[j]+", "+user.userSimilarity(user_id, neighbor_id[j])+", 1),";
			    	  }
			      }
	    	      sql= sql.substring(0, sql.length()-1);
			      stmt.executeUpdate(sql);
		      }
		      System.out.println("Neighbors have been inserted");
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

	public static void main(String[] args) throws IOException, TasteException {
		Behavior_neighbor gen = new Behavior_neighbor();
		gen.clear_data();
		gen.write_data();
    	gen.write_neighborhood();
	}
}

