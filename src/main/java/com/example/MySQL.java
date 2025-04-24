package com.example;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.ResultSet;
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
            while (rs.next()) {
                // Example: print first column
                System.out.println(rs.getString(1)+" | " + rs.getString(2) + " | "+ rs.getString(3));
            }
            obj.addProperty("time", time);
            try {
                FileWriter writer=new FileWriter("SQLLog.jsonl",true);
                writer.write(obj.toString()+System.lineSeparator());
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
                FileWriter writer=new FileWriter("SQLLog.jsonl",true);
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
