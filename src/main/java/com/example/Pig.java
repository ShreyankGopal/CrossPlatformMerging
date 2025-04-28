package com.example;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import com.google.gson.JsonObject;

public class Pig {

    private final String baseLoadCommand = 
        "Grades = LOAD 'src/main/java/com/example/grades.csv' " +
        "USING PigStorage(',') AS (studentID:chararray, subjectCode:chararray, grade:chararray);\n\n";

    public void getResult(String pigQuery,JsonObject obj,int time) {
        obj.addProperty("time", time);
        runPigScriptForGet(pigQuery);
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
    private void runPigScriptForGet(String pigCommands){
        try{
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
        }
        catch(IOException | InterruptedException e){
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

        // ===== START: Update grades.csv =====
        String originalFilePath = "src/main/java/com/example/grades.csv";
        String updatedFilePath = "src/main/java/com/example/updated_grades.csv/part-m-00000";
        // String finalOutputPath = "src/main/java/com/example/grades.csv";  // overwrite original grades.csv

        Map<String, String> updates = new HashMap<>();

        // Read updated grades into a Map
        try (BufferedReader br = new BufferedReader(new FileReader(updatedFilePath))) {
            String line;
            int cnt=0;
            while ((line = br.readLine())!=null && cnt==0) {
                String[] parts = line.split(",");
                if (parts.length >= 3) {
                    String key = parts[0] + "," + parts[1]; // studentID,subjectCode
                    updates.put(key, parts[2]);  // grade
                }
                cnt++;
            }
        }

        // Create a temp file to store final output
        File tempFile = new File(System.getProperty("java.io.tmpdir") + "/temp_grades.csv");
        try (
            BufferedReader br = new BufferedReader(new FileReader(originalFilePath));
            BufferedWriter bw = new BufferedWriter(new FileWriter(tempFile))
        ) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split(",");
                if (parts.length >= 3) {
                    String key = parts[0] + "," + parts[1];
                    if (updates.containsKey(key)) {
                        parts[2] = updates.get(key); // Update grade
                    }
                    bw.write(String.join(",", parts));
                    bw.newLine();
                }
            }
        }

        // Replace original grades.csv with updated temp file
        tempFile.renameTo(new File(originalFilePath));
        System.out.println("grades.csv successfully updated!");
        File updatedGradesDir = new File("src/main/java/com/example/updated_grades.csv");
        if (updatedGradesDir.exists()) {
            deleteDirectory(updatedGradesDir);
        }

        

        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
    private void deleteDirectory(File directory) {
        File[] files = directory.listFiles();
        if (files != null) {
            for (File f : files) {
                deleteDirectory(f); // recursive delete
            }
        }
        directory.delete();
    }   
}
