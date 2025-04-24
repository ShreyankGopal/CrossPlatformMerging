package com.example;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;

import com.google.gson.JsonObject;

public class Pig {

    private final String baseLoadCommand = 
        "grades = LOAD '/Users/SGBHAT/Library/CloudStorage/OneDrive-iiit-b/IIIT-B/sem6/NOSql/Project/project/src/main/java/com/example/grades.csv' " +
        "USING PigStorage(',') AS (studentId:chararray, courseId:chararray, grade:chararray);\n";

    public void getResult(String pigQuery,JsonObject obj,int time) {
        obj.addProperty("time", time);
        runPigScript(pigQuery);
        try {
            FileWriter writer=new FileWriter("PigLog.jsonl",true);
            writer.write(obj.toString()+System.lineSeparator());
            writer.close();
        } catch (Exception e) {
            System.out.println(e);
        }
    }

    public void setResult(String pigQuery,JsonObject obj,int time) {
        obj.addProperty("time", time);
        runPigScript(pigQuery);
        try {
            FileWriter writer=new FileWriter("PigLog.jsonl",true);
            writer.write(obj.toString()+System.lineSeparator());
            writer.close();
        } catch (Exception e) {
            System.out.println(e);
        }
    }

    private void runPigScript(String pigCommands) {
        try {
            // Create a temporary Pig script file
            File pigScript = File.createTempFile("pigscript", ".pig");
            try (FileWriter writer = new FileWriter(pigScript)) {
                writer.write(baseLoadCommand);
                writer.write(pigCommands + "\n");
            }

            // Execute the Pig script locally
            ProcessBuilder processBuilder = new ProcessBuilder("pig", "-x", "local", pigScript.getAbsolutePath());
            processBuilder.redirectErrorStream(true);

            System.out.println("Executing Pig script: " + pigScript.getAbsolutePath());
            Process process = processBuilder.start();

            // Output the process results
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    System.out.println(line);
                }
            }

            int exitCode = process.waitFor();
            System.out.println("Pig script executed with exit code: " + exitCode);

            // Delete the temporary file
            pigScript.delete();

        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
