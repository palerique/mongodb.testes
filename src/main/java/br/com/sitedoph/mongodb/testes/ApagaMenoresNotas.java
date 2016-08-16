package br.com.sitedoph.mongodb.testes;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

import static com.mongodb.client.model.Sorts.ascending;

/**
 * Created by ph on 8/15/16.
 */
public class ApagaMenoresNotas {

    public static final String STUDENT_ID = "student_id";
    public static final String SCORE = "score";
    public static final String TYPE = "type";
    public static final String HOMEWORK = "homework";

    public static void main(String[] args) {
        MongoClient client = new MongoClient();

        MongoDatabase database = client.getDatabase("students");
        final MongoCollection<Document> collection = database.getCollection("grades");

//        Hint/spoiler: If you select homework grade-documents, sort by student and then by score, you can iterate
//        through and find the lowest score for each student by noticing a change in student id.
//        As you notice that change of student_id, remove the document.

        List<Document> results =
                collection.find(new Document(TYPE, HOMEWORK)).sort(ascending(STUDENT_ID, SCORE))
                        .into(new ArrayList<Document>());

        int currentStudentId = 0;

        System.out.println(results.size());

        for (Document cur : results) {

            Integer iterStudentiID = cur.getInteger(STUDENT_ID);

            if (currentStudentId != iterStudentiID) {
                System.out.println(cur);
                collection.deleteOne(cur);
                currentStudentId = iterStudentiID;
            }

        }
    }
}
