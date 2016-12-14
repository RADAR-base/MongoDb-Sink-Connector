/*
 *  Copyright 2016 Kings College London and The Hyve
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.radarcns.mongodb;

import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.MongoException;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UpdateOptions;

import org.apache.kafka.connect.errors.ConnectException;
import org.bson.Document;
import org.radarcns.util.Utility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.mongodb.client.model.Filters.eq;
import static org.apache.kafka.connect.sink.SinkTask.TOPICS_CONFIG;

/**
 * Wrapper around {@link MongoClient}.
 */
public class MongoWrapper implements Closeable {
    private final Logger log = LoggerFactory.getLogger(MongoWrapper.class);
    private final String dbName;
    private final Map<String, String> mapping;
    private final MongoClient mongoClient;
    private final List<MongoCredential> credentials;

    /**
     * Create a new {@link MongoClient}.
     *
     * @param config Configuration of the client, according to {@link MongoDbSinkConnector}.
     * @throws ConnectException a MongoClient could not be created.
     */
    public MongoWrapper(Map<String, String> config) {
        mapping = Utility.parseArrayConfig(config, TOPICS_CONFIG);
        dbName = config.get(MongoDbSinkConnector.MONGO_DATABASE);
        credentials = createCredentials(config);
        mongoClient = createClient(config);
    }

    private List<MongoCredential> createCredentials(Map<String, String> config) {
        String userName = config.get(MongoDbSinkConnector.MONGO_USERNAME);
        char[] password = config.get(MongoDbSinkConnector.MONGO_PASSWORD).toCharArray();

        return Collections.singletonList(
                MongoCredential.createCredential(userName, dbName, password));
    }

    private MongoClient createClient(Map<String, String> config) {
        String host = config.get(MongoDbSinkConnector.MONGO_HOST);
        int port = Integer.parseInt(config.get(MongoDbSinkConnector.MONGO_PORT));

        try {
            return new MongoClient(new ServerAddress(host, port), credentials);
        } catch (com.mongodb.MongoSocketOpenException e){
            log.error("Failed to create MongoDB client to {}:{} with credential {}", host, port,
                    credentials.get(0), e);
            throw new ConnectException("MongoDb client cannot be created.", e);
        }
    }

    /** Whether the database can be pinged using the current MongoDB client and credentials */
    public boolean checkConnection() {
        try {
            for (MongoCredential user : credentials) {
                mongoClient.getDatabase(user.getSource()).runCommand(new Document("ping", 1));
            }
            return true;
        } catch (Exception e) {
            log.error("Error during MongoDB connection test", e);
            return false;
        }
    }

    @Override
    public void close() {
        mongoClient.close();
        log.info("MongoDB connection is has been closed");
    }

    /**
     * Store a document in MongoDB
     * @param topic Kafka topic that the document belongs to
     * @param doc MongoDB document
     * @throws MongoException if the document could not be stored
     */
    public void store(String topic, Document doc) throws MongoException {
        MongoDatabase database = mongoClient.getDatabase(dbName);
        MongoCollection<Document> collection = database.getCollection(mapping.get(topic));

        collection.replaceOne(eq("_id", doc.get("_id")), doc, (new UpdateOptions()).upsert(true));
    }
}
