package com.xie;

//STEP 1. Import required packages
import java.sql.*;
import java.util.Scanner;

public class drop_table {
 // JDBC driver name and database URL
 static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";  
 static final String DB_URL = "jdbc:mysql://localhost:3306/real_data";
 
 //  Database credentials
 static final String USER = "root";
 static final String PASS = "lxsjw495";
 
 public static void main(String[] args) {
 Connection conn = null;
 Statement stmt = null;
 try{
    //STEP 2: Register JDBC driver
    Class.forName("com.mysql.jdbc.Driver");

    //STEP 3: Open a connection
    System.out.println("Connecting to a selected database...");
    conn = DriverManager.getConnection(DB_URL, USER, PASS);
    System.out.println("Connected database successfully...");
    
    //STEP 4: Execute a query
    System.out.println("Deleting table in given database...");
    stmt = conn.createStatement();
    
    
    System.out.println("Please enter the table name that you want to drop: ");
    Scanner s = new Scanner(System.in); 
    String table_name=s.next();
    s.close();
    String sql = "DROP TABLE "+table_name;

    stmt.executeUpdate(sql);
    System.out.println("Table deleted in given database...");
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
}//end main
}//end JDBCExample
