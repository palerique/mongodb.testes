package br.com.sitedoph.mongodb.m101j.hw3_1;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by ph on 8/15/16.
 */
public class HW3_1 {

    public static final String TYPE = "type";
    public static final String ID = "_id";
    public static final String NAME = "name";
    public static final String SCORES = "scores";
    public static final String HOMEWORK = "homework";
    public static final String SCORE = "score";

    public static void main(String[] args) {

        MongoClient client = new MongoClient();

        MongoDatabase database = client.getDatabase("school");
        final MongoCollection<Document> collection = database.getCollection("students");

        List<Document> results =
                collection.find()
                          .into(new ArrayList<Document>());

        for (Document student : results) {

            int studentId = student.getInteger(ID);
            String name = student.getString(NAME);
            List<Document> scores = (List<Document>) student.get(SCORES);
            System.out.printf("_id[%d], name[%s], scores%s %n", studentId, name, scores);

            Document scoreToRemove = null;
            double minScoreValue = 100.0;

            for (Object obj : scores) {
                Document score = (Document) obj;
                String type = score.getString(TYPE);

                if (!HOMEWORK.equals(type)) {
                    continue;
                }

                double curScoreValue = score.getDouble(SCORE);

                System.out.printf("type[%s], current score value[%f] %n", type, curScoreValue);

                if (curScoreValue < minScoreValue) {
                    scoreToRemove = score;
                    minScoreValue = curScoreValue;
                }

            }

            System.out.printf("score to remove[%s] %n", scoreToRemove);

            if (scoreToRemove != null) {
                scores.remove(scoreToRemove);

                BasicDBObject query = new BasicDBObject(ID, studentId);
                BasicDBObject scoresUpdate = new BasicDBObject(
                        "$set", new BasicDBObject(SCORES, scores));
                UpdateResult result = collection.updateOne(query, scoresUpdate);
                System.out.printf("update count[%d] %n", result.getModifiedCount());
            }
        }
    }

}
