/*
 * Copyright 2017 The Hyve and King's College London
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

package org.radarcns.connect.mongodb;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.UpdateOptions;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.errors.ConnectException;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.mongodb.client.model.Filters.eq;
import static org.radarcns.connect.mongodb.MongoDbSinkConnector.COLLECTION_FORMAT;
import static org.radarcns.connect.mongodb.MongoDbSinkConnector.MONGO_URI;

/**
 * Wrapper around {@link MongoClient}.
 */
public class MongoWrapper implements Closeable {
    private final Logger log = LoggerFactory.getLogger(MongoWrapper.class);
    private final MongoClient mongoClient;
    private final String collectionFormat;
    private final MongoDatabase database;

    private static final UpdateOptions UPDATE_UPSERT = new UpdateOptions().upsert(true);
    private final Map<String, MongoCollection<Document>> collectionCache;

    private static final String MONGO_ID_KEY = "_id";

    /**
     * Create a new {@link MongoClient}.
     *
     * @param config Configuration of the client, according to {@link MongoDbSinkConnector}.
     * @throws ConnectException a MongoClient could not be created.
     */
    public MongoWrapper(AbstractConfig config, MongoClientOptions.Builder options) {
        collectionFormat = config.getString(COLLECTION_FORMAT);
        MongoClientURI mongoUri = new MongoClientURI(config.getString(MONGO_URI),
                options == null ? new MongoClientOptions.Builder() : options);
        collectionCache = new HashMap<>();
        mongoClient = createClient(mongoUri);
        database = mongoClient.getDatabase(mongoUri.getDatabase());
    }

    private MongoClient createClient(MongoClientURI uri) {
        try {
            return new MongoClient(uri);
        } catch (MongoException ex) {
            log.error("Failed to create MongoDB client to {}", uri, ex);
            throw new ConnectException("MongoDb client cannot be created.", ex);
        }
    }

    /** Whether the database can be pinged using the current MongoDB client and credentials. */
    public boolean checkConnection() {
        try {
            database.runCommand(new Document("ping", 1));
            return true;
        } catch (Exception ex) {
            log.error("Error during MongoDB connection test", ex);
            return false;
        }
    }

    @Override
    public void close() {
        mongoClient.close();
        log.info("MongoDB connection is has been closed");
    }

    /**
     * Store a document in MongoDB. If the document has an ID, it will replace existing
     * documents with that ID. If it does not, it will be inserted and MongoDB will assign it with
     * a unique ID.
     *
     * @param topic Kafka topic that the document belongs to
     * @param doc MongoDB document
     * @throws MongoException if the document could not be stored.
     */
    public void store(String topic, Document doc) throws MongoException {
        MongoCollection<Document> collection = getCollection(topic);
        Object mongoId = doc.get(MONGO_ID_KEY);
        if (mongoId != null) {
            collection.replaceOne(eq(MONGO_ID_KEY, mongoId), doc, UPDATE_UPSERT);
        } else {
            collection.insertOne(doc);
        }
    }

    /**
     * Stores all documents in the stream. Documents that have an ID will replace existing
     * documents with that ID, documents without ID will be inserted and MongoDB will assign it with
     * a unique ID.
     * @param topic topic to store the documents for
     * @param docs documents to insert.
     * @throws MongoException if a document could not be stored.
     */
    public void store(String topic, Stream<Document> docs) throws MongoException {
        getCollection(topic).bulkWrite(docs
                .map(doc -> {
                    Object mongoId = doc.get(MONGO_ID_KEY);
                    if (mongoId != null) {
                        return new ReplaceOneModel<>(eq(MONGO_ID_KEY, mongoId), doc, UPDATE_UPSERT);
                    } else {
                        return new InsertOneModel<>(doc);
                    }
                })
                .collect(Collectors.toList()));
    }

    private MongoCollection<Document> getCollection(String topic) {
        return collectionCache.computeIfAbsent(topic,
                t -> database.getCollection(collectionFormat.replace("{$topic}", t)));
    }

    /**
     * Retrieves all documents in a collection.
     */
    public MongoIterable<Document> getDocuments(String topic) {
        return getCollection(topic).find();
    }
}
