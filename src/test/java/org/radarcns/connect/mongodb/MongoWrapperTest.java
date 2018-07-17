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

import com.mongodb.MongoClientOptions;
import com.mongodb.MongoException;
import org.apache.kafka.common.config.AbstractConfig;
import org.bson.Document;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.connect.sink.SinkConnector.TOPICS_CONFIG;
import static org.junit.Assert.assertFalse;
import static org.radarcns.connect.mongodb.MongoDbSinkConnector.COLLECTION_FORMAT;
import static org.radarcns.connect.mongodb.MongoDbSinkConnector.CONFIG_DEF;
import static org.radarcns.connect.mongodb.MongoDbSinkConnector.MONGO_URI;

public class MongoWrapperTest {
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void checkConnectionWithoutCredentials() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(MONGO_URI, "mongodb://localhost:28017/mydb");
        configMap.put(COLLECTION_FORMAT, "{$topic}");
        configMap.put(TOPICS_CONFIG, "a");

        MongoClientOptions.Builder timeout = MongoClientOptions.builder()
                .connectTimeout(1)
                .socketTimeout(1)
                .serverSelectionTimeout(1);
        MongoWrapper wrapper = new MongoWrapper(new AbstractConfig(CONFIG_DEF, configMap), timeout);

        assertFalse(wrapper.checkConnection());

        thrown.expect(MongoException.class);
        try {
            wrapper.store("mytopic", new Document());
        } finally {
            wrapper.close();
        }
    }

    @Test
    public void checkConnectionWithCredentials() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(MONGO_URI, "mongodb://myuser:mypassword@localhost:28017/mydb");
        configMap.put(COLLECTION_FORMAT, "{$topic}");
        configMap.put(TOPICS_CONFIG, "a");

        MongoClientOptions.Builder timeout = MongoClientOptions.builder()
                .connectTimeout(1)
                .socketTimeout(1)
                .serverSelectionTimeout(1);
        MongoWrapper wrapper = new MongoWrapper(new AbstractConfig(CONFIG_DEF, configMap), timeout);

        assertFalse(wrapper.checkConnection());

        thrown.expect(MongoException.class);
        try {
            wrapper.store("mytopic", new Document());
        } finally {
            wrapper.close();
        }
    }
}