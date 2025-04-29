package com.example;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import com.google.gson.JsonObject;
public class MySQL {
    public Connection conn;
    public Statement stmt;
    public MySQL(Connection conn,Statement stmt){
        this.conn=conn;
        this.stmt=stmt;
    }
    public void getResult(String query,JsonObject obj,int time){
        try {
            ResultSet rs = stmt.executeQuery(query);
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();
            
            while (rs.next()) {
                for (int i = 1; i <= columnCount; i++) {
                    System.out.print(rs.getString(i));
                    if (i < columnCount) {
                        System.out.print(" | ");
                    }
                }
                System.out.println();
            }
            
            obj.addProperty("time", time);
            try {
                FileWriter writer = new FileWriter("MySQLLog.jsonl", true);
                writer.write(obj.toString() + System.lineSeparator());
                writer.close();
            } catch (Exception e) {
                System.out.println(e);
            }
            
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    public void setResult(String update,JsonObject obj,int time){
        try{
            stmt.executeUpdate(update);
            obj.addProperty("time", time);
            try {
                FileWriter writer=new FileWriter("MySQLLog.jsonl",true);
                writer.write(obj.toString()+System.lineSeparator());
                writer.close();
            } catch (Exception e) {
                System.out.println(e);
            }
        } catch(SQLException e){
            e.printStackTrace();
        }
    }
}
