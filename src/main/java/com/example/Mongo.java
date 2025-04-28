package com.example;
import java.io.FileWriter;

import org.bson.Document;

import com.google.gson.JsonObject;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
public class Mongo{
    public MongoClient mongoClient;
    public MongoDatabase database;
    public Mongo(MongoClient mongoClient,MongoDatabase database){
        this.mongoClient=mongoClient;
        this.database=database;
    }
    public void getResult(String collectionName, String filterJson,JsonObject obj,int time) {
        try {
            MongoCollection<Document> collection = database.getCollection(collectionName);

            Document filter = Document.parse(filterJson);  // Parse JSON string to Document
            FindIterable<Document> results = collection.find(filter);

            for (Document doc : results) {
                //System.out.println("------------------------MONGO----------------------");
                System.out.println(doc.toJson());
            }
            obj.addProperty("time", time);
            FileWriter writer=new FileWriter("MongoLog.jsonl",true);
            writer.write(obj.toString()+System.lineSeparator());
            writer.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public void setResult(String collectionName, String filterJson, String updateJson,JsonObject obj,int time) {
        try {
            MongoCollection<Document> collection = database.getCollection(collectionName);
            Document filter = Document.parse(filterJson);
            Document update = new Document("$set", Document.parse(updateJson));
            collection.updateOne(filter, update);
            System.out.println("Update applied to: " + filter.toJson());
            obj.addProperty("time", time);
            FileWriter writer = new FileWriter("MongoLog.jsonl", true); // true enables append mode
            writer.write(obj.toString() + System.lineSeparator());
            writer.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    } 

}