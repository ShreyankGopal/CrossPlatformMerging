package com.example;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

public class MergeHandler {

    // Global timestamp counter
    public static int globalCounter = 1;

    // Global oplog line counters
    public static int mysqlOplogLineCount = 0;
    public static int mongoOplogLineCount = 0;
    public static int pigOplogLineCount = 0;

    // Global file pointers (fromPointer, toPointer)
    public static Map<String, int[]> pointerPairs = new HashMap<>();

    // Global merged flags
    public static Map<String, Boolean> mergedFlags = new HashMap<>();
    
    static {
        // Initialize pointer pairs
        pointerPairs.put("mysql->mongo", new int[]{0, 0});
        pointerPairs.put("mysql->pig", new int[]{0, 0});
        pointerPairs.put("mongo->mysql", new int[]{0, 0});
        pointerPairs.put("mongo->pig", new int[]{0, 0});
        pointerPairs.put("pig->mysql", new int[]{0, 0});
        pointerPairs.put("pig->mongo", new int[]{0, 0});

        // Initialize merge flags
        mergedFlags.put("mysql_merged_with_mongo", false);
        mergedFlags.put("mysql_merged_with_pig", false);
        mergedFlags.put("mongo_merged_with_mysql", false);
        mergedFlags.put("mongo_merged_with_pig", false);
        mergedFlags.put("pig_merged_with_mysql", false);
        mergedFlags.put("pig_merged_with_mongo", false);
    }

    public static void merge(String from, String to) throws IOException {
        System.out.println("-- Merging from " + from + " to " + to);

        String fromFile = getOplogFile(from);
        String toFile = getOplogFile(to);
        int[] pointers = pointerPairs.get(from.toLowerCase() + "->" + to.toLowerCase());

        int fromPointer = pointers[0];
        int toPointer = pointers[1];

        List<JsonObject> fromOps = readLatestOps(fromFile, fromPointer, from);
        List<JsonObject> toOps = readLatestOps(toFile, toPointer, to);

        Map<String, JsonObject> fromMap = buildLatestSetMap(fromOps);
        Map<String, JsonObject> toMap = buildLatestSetMap(toOps);

        FileWriter toWriter = new FileWriter(toFile, true); // append mode
        Gson gson = new Gson();

        for (String key : fromMap.keySet()) {
            JsonObject fromOp = fromMap.get(key);

            if (!toMap.containsKey(key)) {
                // Not present → Insert
                JsonObject newOp = createNewSetOp(to, fromOp);
                toWriter.write(gson.toJson(newOp) + "\n");
                incrementLineCounter(to);
            } else {
                JsonObject toOp = toMap.get(key);
                String fromVal = fromOp.getAsJsonArray("column_values").toString();
                String toVal = toOp.getAsJsonArray("column_values").toString();
                int fromTime = fromOp.get("time").getAsInt();
                int toTime = toOp.get("time").getAsInt();

                if (!fromVal.equals(toVal) && fromTime > toTime) {
                    // Conflict → overwrite
                    JsonObject newOp = createNewSetOp(to, fromOp);
                    toWriter.write(gson.toJson(newOp) + "\n");
                    incrementLineCounter(to);
                }
            }
        }

        toWriter.close();

        // Update pointers to file end
        pointers[0] = getCurrentLineCount(from);
        pointers[1] = getCurrentLineCount(to);

        pointerPairs.put(from.toLowerCase() + "->" + to.toLowerCase(), pointers);

        // Update merged flags
        updateMergedFlags(from, to);

        // Check for full convergence
        if (allSystemsMerged()) {
            flushOplogs();
            resetAll();
            System.out.println("-- Systems are fully merged. Oplogs flushed.");
        }
    }

    private static List<JsonObject> readLatestOps(String filePath, int pointerLine, String db) throws IOException {
        List<JsonObject> operations = new ArrayList<>();
        RandomAccessFile raf = new RandomAccessFile(filePath, "r");
        long fileLength = raf.length();
        long pointer = fileLength - 1;

        int totalLines = getCurrentLineCount(db);
        int linesToRead = totalLines - pointerLine;
        int linesRead = 0;

        StringBuilder sb = new StringBuilder();
        Gson gson = new Gson();

        while (pointer >= 0 && linesRead < linesToRead) {
            raf.seek(pointer);
            char c = (char) raf.read();
            if (c == '\n') {
                if (sb.length() > 0) {
                    sb.reverse();
                    String line = sb.toString();
                    JsonObject obj = gson.fromJson(line, JsonObject.class);
                    if (obj.has("op") && obj.get("op").getAsString().equals("SET")) {
                        operations.add(obj);
                    }
                    linesRead++;
                    sb = new StringBuilder();
                }
            } else {
                sb.append(c);
            }
            pointer--;
        }

        // Handle first line if needed (if file did not end with a \n)
        if (sb.length() > 0 && linesRead < linesToRead) {
            sb.reverse();
            String line = sb.toString();
            JsonObject obj = gson.fromJson(line, JsonObject.class);
            if (obj.has("op") && obj.get("op").getAsString().equals("SET")) {
                operations.add(obj);
            }
        }

        raf.close();
        return operations;
    }

    private static Map<String, JsonObject> buildLatestSetMap(List<JsonObject> ops) {
        Map<String, JsonObject> map = new HashMap<>();

        for (JsonObject obj : ops) {
            String compositeKey = buildKey(obj);
            if (!map.containsKey(compositeKey)) {
                map.put(compositeKey, obj); // first seen latest op wins
            }
        }
        return map;
    }

    private static String buildKey(JsonObject obj) {
        JsonArray keyAttrs = obj.getAsJsonArray("key_attributes");
        JsonArray keyVals = obj.getAsJsonArray("key_values");
        JsonArray colAttrs = obj.getAsJsonArray("column_attributes");

        List<String> parts = new ArrayList<>();
        for (int i = 0; i < keyAttrs.size(); i++) {
            parts.add(keyAttrs.get(i).getAsString() + "=" + keyVals.get(i).getAsString());
        }
        for (int i = 0; i < colAttrs.size(); i++) {
            parts.add(colAttrs.get(i).getAsString());
        }

        return String.join("|", parts);
    }

    private static JsonObject createNewSetOp(String toDb, JsonObject sourceOp) {
        JsonObject newOp = sourceOp.deepCopy();
        newOp.addProperty("db", toDb);
        newOp.addProperty("time", globalCounter++);
        return newOp;
    }

    private static void incrementLineCounter(String db) {
        if (db.equalsIgnoreCase("SQL")) mysqlOplogLineCount++;
        else if (db.equalsIgnoreCase("MONGO")) mongoOplogLineCount++;
        else if (db.equalsIgnoreCase("PIG")) pigOplogLineCount++;
    }

    private static int getCurrentLineCount(String db) {
        if (db.equalsIgnoreCase("SQL")) return mysqlOplogLineCount;
        else if (db.equalsIgnoreCase("MONGO")) return mongoOplogLineCount;
        else return pigOplogLineCount;
    }

    private static String getOplogFile(String db) {
        if (db.equalsIgnoreCase("SQL")) return "SQLLog.jsonl";
        else if (db.equalsIgnoreCase("MONGO")) return "MongoLog.jsonl";
        else return "PigLog.jsonl";
    }

    private static void updateMergedFlags(String from, String to) {
        mergedFlags.put(from.toLowerCase() + "merged_with" + to.toLowerCase(), true);

        // Check transitive relations
        if (from.equalsIgnoreCase("pig") && mergedFlags.get("mongo_merged_with_pig")) {
            mergedFlags.put("mongo_merged_with_" + to.toLowerCase(), true);
        }
        if (from.equalsIgnoreCase("mongo") && mergedFlags.get("pig_merged_with_mongo")) {
            mergedFlags.put("pig_merged_with_" + to.toLowerCase(), true);
        }
        if (from.equalsIgnoreCase("mysql") && mergedFlags.get("mongo_merged_with_mysql")) {
            mergedFlags.put("mongo_merged_with_" + to.toLowerCase(), true);
        }
    }

    private static boolean allSystemsMerged() {
        for (Boolean merged : mergedFlags.values()) {
            if (!merged) return false;
        }
        return true;
    }

    private static void flushOplogs() throws IOException {
        new FileWriter("SQLLog.jsonl", false).close();
        new FileWriter("MongoLog.jsonl", false).close();
        new FileWriter("PigLog.jsonl", false).close();

        mysqlOplogLineCount = 0;
        mongoOplogLineCount = 0;
        pigOplogLineCount = 0;
    }

    private static void resetAll() {
        for (String key : pointerPairs.keySet()) {
            pointerPairs.put(key, new int[]{0, 0});
        }
        for (String key : mergedFlags.keySet()) {
            mergedFlags.put(key, false);
        }
    }
}