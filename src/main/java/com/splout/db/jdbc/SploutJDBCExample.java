package com.splout.db.jdbc;


import java.sql.*;

public class SploutJDBCExample {

  public static void main(String args[]) throws ClassNotFoundException, SQLException {
    Class driver = Class.forName("com.splout.db.jdbc.SimpleSploutJDBCDriver");
    Connection conn = DriverManager.getConnection("jdbc:splout://localhost:4412?key='country_code'&tablespace='city_pby_country_code'");
    Statement stmt = conn.createStatement();
    ResultSet rs = stmt.executeQuery("select * from city where country_code = 'AFG';");
    while(rs.next()){
      for (int i=1; i<6; i++) {
        System.out.print(rs.getObject(i) + "\t");
      }
      System.out.println();
    }
    stmt.close();
    conn.close();
  }
}
