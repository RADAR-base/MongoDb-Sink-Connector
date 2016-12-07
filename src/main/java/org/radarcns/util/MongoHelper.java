package org.radarcns.util;

import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.MongoException;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UpdateOptions;

import org.apache.kafka.connect.errors.ConnectException;
import org.bson.Document;
import org.radarcns.mongodb.MongoDbSinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.mongodb.client.model.Filters.eq;
import static java.util.Collections.singletonList;
import static org.apache.kafka.connect.sink.SinkTask.TOPICS_CONFIG;

/**
 * Created by Francesco Nobilia on 28/11/2016.
 */
public class MongoHelper {

    private final Logger log = LoggerFactory.getLogger(MongoHelper.class);

    private MongoClient mongoClient;
    private String dbName;

    private Map<String, String> mapping;

    public MongoHelper(Map<String, String> config){
        initClient(config);

        mapping = new HashMap<>();
        for (String topic : Utility.stringToSet(config.get(TOPICS_CONFIG))) {
            mapping.put(topic, config.get(topic));
        }
    }

    public void initClient(Map<String, String> config){
        try {
            List<MongoCredential> credentials = getMongoDBUser(config);

            mongoClient = new MongoClient(getMongoDBHosts(config), credentials);

            if (checkMongoConnection(mongoClient, credentials)) {
                log.info("MongoDB connection established");

                dbName = config.get(MongoDbSinkConnector.DB);
            }
        }
        catch (com.mongodb.MongoSocketOpenException e){
            if(mongoClient != null) {
                mongoClient.close();
            }

            log.error(e.getMessage());
            throw new ConnectException("MongoDb client cannot be created.", e);
        }
    }

    private boolean checkMongoConnection(MongoClient mongoClient, List<MongoCredential> credentials){
        Boolean flag = true;
        try {
            for(MongoCredential user : credentials) {
                mongoClient.getDatabase(user.getSource()).runCommand(new Document("ping", 1));
            }

        } catch (Exception e) {
            flag = false;

            if(mongoClient != null) {
                mongoClient.close();
            }

            log.error("Error during connection test", e);
        }

        log.info("MongoDB connection is {}", flag.toString());

        return flag;
    }

    private List<MongoCredential> getMongoDBUser(Map<String, String> config){
        return singletonList(MongoCredential.createCredential(
                config.get(MongoDbSinkConnector.USR),
                config.get(MongoDbSinkConnector.DB),
                config.get(MongoDbSinkConnector.PWD).toCharArray()));
    }

    private List<ServerAddress> getMongoDBHosts(Map<String, String> config){
        List<ServerAddress> mongoHostsTemp = new LinkedList<>();
        mongoHostsTemp.add(new ServerAddress(config.get(MongoDbSinkConnector.HOST), Integer.valueOf(config.get(MongoDbSinkConnector.PORT))));
        return mongoHostsTemp;
    }

    private MongoCollection<Document> getCollection(MongoClient mongoClient, String dbName, String collection){
        MongoDatabase database = mongoClient.getDatabase(dbName);
        return database.getCollection(collection);
    }

    public void close(){
        if(mongoClient != null) {
            mongoClient.close();
            log.info("MongoDB connection is has been closed");
        }
        else{
            log.warn("Impossible to close a null mongoDB client");
        }
    }

    public void store(String topic, Document doc) throws MongoException {
        MongoCollection<Document> collection = getCollection(mongoClient, dbName, mapping.get(topic));

        collection.replaceOne(eq("_id", doc.get("_id")), doc, (new UpdateOptions()).upsert(true));
    }
}
