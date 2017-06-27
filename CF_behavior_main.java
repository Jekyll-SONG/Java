package com.xie.behavior;

import java.io.IOException;

import org.apache.mahout.cf.taste.common.TasteException;

public class CF_behavior_main {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		// read from db into CSV
		
		Write_behavior_into_CF gen = new Write_behavior_into_CF();
		try {
			gen.clear_data();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		gen.write_data();
		
		System.out.println("Read from db : Success");
		
    	Write_behavior_neighborhood item=new Write_behavior_neighborhood();
    	item.create_table();
    	try {
			item.write_neighborhood();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TasteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	System.out.println("Write into db : Success");
    	
    	
	}

}
