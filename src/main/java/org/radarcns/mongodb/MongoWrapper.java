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
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.MongoException;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UpdateOptions;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.errors.ConnectException;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Collections;
import java.util.List;

import static com.mongodb.client.model.Filters.eq;
import static org.radarcns.mongodb.MongoDbSinkConnector.COLLECTION_FORMAT;
import static org.radarcns.mongodb.MongoDbSinkConnector.MONGO_DATABASE;
import static org.radarcns.mongodb.MongoDbSinkConnector.MONGO_HOST;
import static org.radarcns.mongodb.MongoDbSinkConnector.MONGO_PASSWORD;
import static org.radarcns.mongodb.MongoDbSinkConnector.MONGO_PORT;
import static org.radarcns.mongodb.MongoDbSinkConnector.MONGO_USERNAME;

/**
 * Wrapper around {@link MongoClient}.
 */
public class MongoWrapper implements Closeable {
    private final Logger log = LoggerFactory.getLogger(MongoWrapper.class);
    private final String dbName;
    private final MongoClient mongoClient;
    private final List<MongoCredential> credentials;
    private final String collectionFormat;

    /**
     * Create a new {@link MongoClient}.
     *
     * @param config Configuration of the client, according to {@link MongoDbSinkConnector}.
     * @throws ConnectException a MongoClient could not be created.
     */
    public MongoWrapper(AbstractConfig config, MongoClientOptions options) {
        collectionFormat = config.getString(COLLECTION_FORMAT);
        dbName = config.getString(MONGO_DATABASE);
        credentials = createCredentials(config);
        mongoClient = createClient(config, options);
    }

    private List<MongoCredential> createCredentials(AbstractConfig config) {
        String userName = config.getString(MONGO_USERNAME);
        String password = config.getString(MONGO_PASSWORD);
        if (isValid(userName) && isValid(password)) {
            return Collections.singletonList(
                    MongoCredential.createCredential(userName, dbName, password.toCharArray()));
        } else {
            return Collections.emptyList();
        }
    }

    private boolean isValid(String value) {
        return value != null && !value.isEmpty();
    }

    private MongoClient createClient(AbstractConfig config, MongoClientOptions options) {
        String host = config.getString(MONGO_HOST);
        int port = config.getInt(MONGO_PORT);

        try {
            if (options == null) {
                options = new MongoClientOptions.Builder().build();
            }
            return new MongoClient(new ServerAddress(host, port), credentials, options);
        } catch (com.mongodb.MongoSocketOpenException e){
            log.error("Failed to create MongoDB client to {}:{} with credentials {}", host, port,
                    credentials, e);
            throw new ConnectException("MongoDb client cannot be created.", e);
        }
    }

    /** Whether the database can be pinged using the current MongoDB client and credentials */
    public boolean checkConnection() {
        try {
            mongoClient.getDatabase(dbName).runCommand(new Document("ping", 1));
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
        String collectionName = collectionFormat.replace("{$topic}", topic);
        MongoCollection<Document> collection = database.getCollection(collectionName);

        collection.replaceOne(eq("_id", doc.get("_id")), doc, (new UpdateOptions()).upsert(true));
    }
}
