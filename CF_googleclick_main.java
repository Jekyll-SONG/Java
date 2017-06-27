package com.xie.googleclick;

import java.util.List;

public class CF_googleclick_main {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Write_data gen = new Write_data();
		gen.create_user_knowledge_table();
		List<hibernate.UserKnowledge> recordlist  = gen.get_record_list();
		gen.write_to_User_knowledge(recordlist);
		
		System.out.println("success");
 
		// avg 
		Eliminate_dup_svg gen_avg = new Eliminate_dup_svg();
		gen_avg.create_user_knowledge_avg();
		
		List<hibernate.UserKnowledge> recordlist_avg=gen_avg.get_record_list();
		List<hibernate.UserKnowledgeWithoutDuplicate> recordlist_witho_dup =gen_avg.calculate_prob(recordlist_avg);
		gen_avg.save_to_User_knowledge(recordlist_witho_dup);
		System.out.println("success");
	}

}
