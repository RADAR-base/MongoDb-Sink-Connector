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

import org.apache.kafka.common.config.ConfigException;
import org.junit.Test;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.connect.sink.SinkConnector.TOPICS_CONFIG;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.radarcns.connect.mongodb.MongoDbSinkConnector.BUFFER_CAPACITY;
import static org.radarcns.connect.mongodb.MongoDbSinkConnector.COLLECTION_FORMAT;
import static org.radarcns.connect.mongodb.MongoDbSinkConnector.MONGO_URI;

public class MongoDbSinkConnectorTest {
    @Test
    public void taskClass() {
        assertEquals(MongoDbSinkTask.class, new MongoDbSinkConnector().taskClass());
    }

    @Test
    public void taskConfigs() {
        assertThat(new MongoDbSinkConnector().taskConfigs(0), hasSize(0));
        assertThat(new MongoDbSinkConnector().taskConfigs(10), hasSize(10));
    }

    @Test
    public void configParse() {
        Map<String, String> exampleConfig = new HashMap<>();
        exampleConfig.put(MONGO_URI, "mongodb://localhost/mydb");
        exampleConfig.put(TOPICS_CONFIG, "mytopic,myothertopic");
        exampleConfig.put(BUFFER_CAPACITY, "2");
        Map<String, Object> result = new MongoDbSinkConnector().config().parse(exampleConfig);
        assertEquals(2, result.get(BUFFER_CAPACITY));
        assertThat((Collection<?>)result.get(TOPICS_CONFIG), contains("mytopic", "myothertopic"));
    }

    @Test(expected = ConfigException.class)
    public void configParseInvalidValue() throws Exception {
        Map<String, String> exampleConfig = new HashMap<>();
        exampleConfig.put(MONGO_URI, "mongodb://localhost/mydb");
        exampleConfig.put(TOPICS_CONFIG, "mytopic,myothertopic");
        // capacity is invalid
        exampleConfig.put(BUFFER_CAPACITY, "-1");

        new MongoDbSinkConnector().config().parse(exampleConfig);
    }

    @Test(expected = ConfigException.class)
    public void configParseEmptyValue() throws Exception {
        Map<String, String> exampleConfig = new HashMap<>();
        exampleConfig.put(MONGO_URI, "mongodb://localhost/mydb");
        exampleConfig.put(TOPICS_CONFIG, "mytopic,myothertopic");
        // empty string not allowed
        exampleConfig.put(COLLECTION_FORMAT, "");

        new MongoDbSinkConnector().config().parse(exampleConfig);
    }

    @Test
    public void start() {
        Map<String, String> exampleConfig = new HashMap<>();
        exampleConfig.put(MONGO_URI, "mongodb://localhost/mydb");
        exampleConfig.put(TOPICS_CONFIG, "mytopic,myothertopic");
        exampleConfig.put(BUFFER_CAPACITY, "2");
        new MongoDbSinkConnector().start(exampleConfig);
    }

    @Test(expected = ConfigException.class)
    public void startInvalid() {
        Map<String, String> exampleConfig = new HashMap<>();
        exampleConfig.put(MONGO_URI, "mongodb:///mydb");
        exampleConfig.put(TOPICS_CONFIG, "mytopic,myothertopic");
        // empty string not allowed
        exampleConfig.put(COLLECTION_FORMAT, "");
        new MongoDbSinkConnector().start(exampleConfig);
    }

    @Test
    public void htmlTable() {
        System.out.println("Configuration table:");
        System.out.println(new MongoDbSinkConnector().config().toHtmlTable());
    }
}
