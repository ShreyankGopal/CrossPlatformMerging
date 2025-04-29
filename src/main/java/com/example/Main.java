package com.example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;

public class Main {
    public Statement stmt;
    public Connection conn;
    public MongoClient mongoClient; // Note: Use MongoClient, not MongoClients
    public MongoDatabase database;

    public static void main(String[] args) {
        Main mainApp = new Main(); 

        mainApp.connectToMongoDB();
        mainApp.connectToMySQL();
       // mainApp.executePigCommands();

        MySQL SQLclass = new MySQL(mainApp.conn, mainApp.stmt);
        Mongo MongoClass = new Mongo(mainApp.mongoClient, mainApp.database);
        Pig PigClass = new Pig();
        MergeHandler Merger = new MergeHandler();
        testCaseReader testReader = new testCaseReader(SQLclass,MongoClass,PigClass);
        testReader.readTest();
    }

    public void connectToMongoDB() {
        String uri = "mongodb://localhost:27017";
        try {
            mongoClient = MongoClients.create(uri);
            database = mongoClient.getDatabase("Project");

            System.out.println("Connected to MongoDB.");
        } catch (Exception e) {
            System.err.println("MongoDB error: " + e.getMessage());
        }
    }

    public void connectToMySQL() {
        String url = "jdbc:mysql://localhost:3306/Project";
        String user = "root";
        String password = "Sghu*560";

        try {
            conn = DriverManager.getConnection(url, user, password);
            stmt = conn.createStatement();
            System.out.println("Connected to MySQL.");

            // ResultSet rs = stmt.executeQuery("SELECT * FROM test");
            // ResultSetMetaData rsmd = rs.getMetaData();
            // int columnsNumber = rsmd.getColumnCount();

            // System.out.println("MySQL test table:");
            // while (rs.next()) {
            //     for (int i = 1; i <= columnsNumber; i++) {
            //         System.out.print(rs.getString(i) + "\t");
            //     }
            //     System.out.println();
            // }
        } catch (Exception e) {
            System.err.println("MySQL error: " + e.getMessage());
        }
    }

    // public void executePigCommands() {
    //     try {
    //         File pigScript = File.createTempFile("pigscript", ".pig");
    //         FileWriter writer = new FileWriter(pigScript);

    //         writer.write("test_data = LOAD '/Users/SGBHAT/Library/CloudStorage/OneDrive-iiit-b/IIIT-B/sem6/NOSql/Project/project/src/main/java/com/example/grades.csv' USING PigStorage(',') AS (field1:chararray, field2:chararray, field3:chararray);\n");
    //         writer.write("DUMP test_data;\n");
    //         writer.close();

    //         ProcessBuilder processBuilder = new ProcessBuilder("pig", "-x", "local", pigScript.getAbsolutePath());
    //         processBuilder.redirectErrorStream(true);

    //         System.out.println("Executing Pig script: " + pigScript.getAbsolutePath());

    //         Process process = processBuilder.start();
    //         BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
    //         String line;
    //         while ((line = reader.readLine()) != null) {
    //             System.out.println(line);
    //         }

    //         int exitCode = process.waitFor();
    //         System.out.println("Pig script executed with exit code: " + exitCode);

    //         pigScript.delete();

    //     } catch (IOException e) {
    //         System.err.println("Error executing Pig commands: " + e.getMessage());
    //         e.printStackTrace();
    //     } catch (InterruptedException e) {
    //         System.err.println("Process interrupted: " + e.getMessage());
    //         e.printStackTrace();
    //     }
    // }
}