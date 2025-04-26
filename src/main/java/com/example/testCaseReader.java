package com.example;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class testCaseReader {
    public MySQL SQLclass;
    public Mongo MongoClass;
    public Pig PigClass;
    public int timeCounter=0;
    public testCaseReader(MySQL SQLclass,Mongo MongoClass,Pig PigClass){
        this.SQLclass=SQLclass;
        this.MongoClass=MongoClass;
        this.PigClass=PigClass;
    }
    public void readTest() {
        String filePath = "/Users/SGBHAT/Library/CloudStorage/OneDrive-iiit-b/IIIT-B/sem6/NOSql/Project/project/src/main/java/com/example/testcase.jsonl"; // Path to your JSON file
        ;


        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            Gson gson = new Gson();

            while ((line = br.readLine()) != null) {
                timeCounter++;
                JsonObject obj = JsonParser.parseString(line).getAsJsonObject();

                if (obj.has("op") && obj.get("op").getAsString().equals("MERGE")) {
                    String from = obj.get("from").getAsString();
                    String to = obj.get("to").getAsString();
                    System.out.println("-- MERGE: Copy data from " + from + " to " + to);
                    continue;
                }

                String db = obj.get("db").getAsString();
                String op = obj.get("op").getAsString();
                JsonArray keyAttrs = obj.getAsJsonArray("key_attributes");
                JsonArray keyVals = obj.getAsJsonArray("key_values");
                String table=obj.get("table").getAsString();
                String condition = "";
                String conditionForPig="";
                for (int i = 0; i < keyAttrs.size(); i++) {
                    if (i > 0) condition += " AND ";
                    condition += keyAttrs.get(i).getAsString() + "='" + keyVals.get(i).getAsString() + "'";
                }
                for (int i = 0; i < keyAttrs.size(); i++) {
                    if (i > 0) conditionForPig += " AND ";
                    conditionForPig += keyAttrs.get(i).getAsString() + "=='" + keyVals.get(i).getAsString() + "'";
                }
                
                if (op.equals("GET")) {
                    switch (db) {
                        case "SQL":
                            
                            System.out.println("SELECT * FROM "+table+" WHERE " + condition + ";");
                            SQLclass.getResult("SELECT * FROM "+table+" WHERE " + condition + ";",obj,timeCounter);
                            break;
                        case "PIG":
                            System.out.println("FILTER " + table + " BY " + conditionForPig + ";");
                            String filteredAlias = table + "_filtered";
                            String pigScript = 
                                    filteredAlias + " = FILTER " + table + " BY " + conditionForPig + ";\n" +
                                    "DUMP " + filteredAlias + ";";

                            PigClass.getResult(pigScript, obj, timeCounter);
                            break;
                        case "MONGO":
                            System.out.println("db.Grades.find({ " + mongoCondition(keyAttrs, keyVals) + " })");
                            String filter="{ "+mongoCondition(keyAttrs, keyVals)+" }";
                            MongoClass.getResult(table,filter,obj,timeCounter);
                            break;
                    }
                    } else if (op.equals("SET")) {
                        JsonArray colAttrs = obj.getAsJsonArray("column_attributes");
                        JsonArray colVals = obj.getAsJsonArray("column_values");

                        String updates = "";
                        for (int i = 0; i < colAttrs.size(); i++) {
                            if (i > 0) updates += ", ";
                            updates += colAttrs.get(i).getAsString() + "='" + colVals.get(i).getAsString() + "'";
                        }
                        String columns = "";
                        for (int i = 0; i < colAttrs.size(); i++) {
                            if (i > 0) columns += ", ";
                            columns += colAttrs.get(i).getAsString();
                        }

                    switch (db) {
                        case "SQL":
                            System.out.println("UPDATE Grades SET " + updates + " WHERE " + condition + ";");
                            SQLclass.setResult("UPDATE Grades SET " + updates + " WHERE " + condition + ";",obj,timeCounter);
                        
                            break;
                        case "PIG":
                            System.out.println("FOREACH "+table+" GENERATE " + updates + " WHERE " + condition + ";");
                            String pigScript = 
                                table + "_filtered = FILTER " + table + " BY " + conditionForPig + ";\n" +
                                table + "_unfiltered = FILTER " + table + " BY NOT " + conditionForPig + ";\n"+
                                table + "_projected = FOREACH " + table + "_filtered GENERATE " + "studentID, subjectCode, "+"'"+colVals.get(0)+"' "+"as grade" + ";\n" +
                                "Final_Grades = UNION Grades_unfiltered, Grades_projected;\n"+
                                "STORE Final_Grades INTO 'file:/Users/SGBHAT/Library/CloudStorage/OneDrive-iiit-b/IIIT-B/sem6/NOSql/Project/project/src/main/java/com/example/updated_grades.csv' USING PigStorage(',');\n";
                                
                                

                                PigClass.setResult(pigScript, obj, timeCounter);
                            break;
                        case "MONGO":
                            System.out.println("db.Grades.updateOne({ " + mongoCondition(keyAttrs, keyVals) + " }, { $set: { " + mongoUpdate(colAttrs, colVals) + " } });");
                            String filter="{ "+mongoCondition(keyAttrs, keyVals)+" }";
                            String update="{ "+mongoUpdate(colAttrs, colVals)+" }";
                            MongoClass.setResult(table,filter,update,obj,timeCounter);
                            break;
                    }
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static String mongoCondition(JsonArray keys, JsonArray values) {
        List<String> pairs = new ArrayList<>();
        for (int i = 0; i < keys.size(); i++) {
            pairs.add("\"" + keys.get(i).getAsString() + "\": \"" + values.get(i).getAsString() + "\"");
        }
        return String.join(", ", pairs);
    }

    private static String mongoUpdate(JsonArray cols, JsonArray vals) {
        List<String> updates = new ArrayList<>();
        for (int i = 0; i < cols.size(); i++) {
            updates.add("\"" + cols.get(i).getAsString() + "\": \"" + vals.get(i).getAsString() + "\"");
        }
        return String.join(", ", updates);
    }
}
