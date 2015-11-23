package com.andresusanto;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Sorts.descending;

import com.mongodb.client.MongoCursor;
import org.bson.Document;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoDatabase;

import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;

public class Main {

    public static void main(String[] args) {
        // command line processor
        String command;
        Scanner scn = new Scanner(System.in);

        // connect to database
        MongoClient client = new MongoClient(new MongoClientURI("mongodb://167.205.35.19:27017,167.205.35.20:27017,167.205.35.21:27017,167.205.35.22:27017/?replicaSet=sister"));
        MongoDatabase db = client.getDatabase("andresusanto");


        System.out.println("Welcome to TweetMongo. (c) 2015 by Andre Susanto 13512028");
        System.out.println("Available commands:");
        System.out.println("\t /reg <username> <password>");
        System.out.println("\t /follow <follower> <user>");
        System.out.println("\t /tweet <username> <what to tweet>");
        System.out.println("\t /duser <username>");
        System.out.println("\t /dtimeline <username>");
        System.out.println("\t /exit");

        while( !(command = scn.next()).equals("/exit") ) {

            if (command.equals("/reg")){
                String username = scn.next();
                String password = scn.next();

                db.getCollection("users").insertOne(new Document().append("username", username).append("password", password));
                System.out.println("Register Success!");
            }else if (command.equals("/follow")){
                String follower = scn.next();
                String user = scn.next();

                db.getCollection("followers").insertOne(new Document().append("username", user).append("follower", follower).append("since", new Date().getTime()));
                db.getCollection("friends").insertOne(new Document().append("username", user).append("friend", follower).append("since", new Date().getTime()));
                db.getCollection("friends").insertOne(new Document().append("friend", user).append("username", follower).append("since", new Date().getTime()));
                System.out.println("Following Success!");
            }else if (command.equals("/tweet")){
                String user = scn.next();
                String tweet = scn.next();
                List<String> following = new LinkedList<String>();
                String tweet_id = user + "_" + new Date().getTime();

                MongoCursor<Document> cursor = db.getCollection("followers").find(eq("username", user)).iterator();
                try {
                    while (cursor.hasNext()) {
                        Document doc = cursor.next();
                        following.add(doc.getString("follower"));
                    }
                } finally {
                    cursor.close();
                }

                db.getCollection("tweets").insertOne(new Document().append("_id", tweet_id).append("username", user).append("body", tweet));
                db.getCollection("userline").insertOne(new Document().append("username", user).append("time", new Date().getTime()).append("tweet_id", tweet_id));
                db.getCollection("timeline").insertOne(new Document().append("username", user).append("time", new Date().getTime()).append("tweet_id", tweet_id));

                for(String followers : following){
                    db.getCollection("timeline").insertOne(new Document().append("username", followers).append("time", new Date().getTime()).append("tweet_id", tweet_id));
                }
                System.out.println("Tweet success!");
            }else if (command.equals("/duser")){
                String user = scn.next();

                MongoCursor<Document> cursor = db.getCollection("userline").find(eq("username", user)).sort(descending("time")).iterator();
                System.out.print(" --- ");
                System.out.print(user);
                System.out.print("'s Userline");
                System.out.println(" --- ");

                try {
                    while (cursor.hasNext()) {
                        Document doc = cursor.next();
                        Document tweet = db.getCollection("tweets").find(eq("_id", doc.getString("tweet_id"))).first();

                        System.out.print(tweet.getString("username"));
                        System.out.print(" : ");
                        System.out.println(tweet.getString("body"));
                    }
                } finally {
                    cursor.close();
                }
                System.out.println("\n --- END ---");
            }else if (command.equals("/dtimeline")){
                String user = scn.next();

                MongoCursor<Document> cursor = db.getCollection("timeline").find(eq("username", user)).sort(descending("time")).iterator();
                System.out.print(" --- ");
                System.out.print(user);
                System.out.print("'s Timeline");
                System.out.println(" --- ");

                try {
                    while (cursor.hasNext()) {
                        Document doc = cursor.next();
                        Document tweet = db.getCollection("tweets").find(eq("_id", doc.getString("tweet_id"))).first();
                        System.out.print(tweet.getString("username"));
                        System.out.print(" : ");
                        System.out.println(tweet.getString("body"));
                    }
                } finally {
                    cursor.close();
                }
                System.out.println(" --- END ---");
            }else{
                System.out.println("Invalid command!");
            }
        }

        client.close();
        System.out.println("Bye bye!");
    }
}
