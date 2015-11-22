package com.andresusanto;

import org.bson.Document;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoDatabase;

public class Main {

    public static void main(String[] args) {
        MongoClient client = new MongoClient(new MongoClientURI("mongodb://167.205.35.19:27017,167.205.35.20:27017,167.205.35.21:27017,167.205.35.22:27017/?replicaSet=rs0"));
        MongoDatabase db = client.getDatabase("andresusanto");

        db.getCollection("coba").insertOne(new Document().append("hebat", "sekali").append("keren", "oke"));
    }
}
