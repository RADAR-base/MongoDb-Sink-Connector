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

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkConnector;
import org.radarcns.serialization.RecordConverterFactory;
import org.radarcns.util.Utility;
import org.radarcns.util.ValidClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.common.config.ConfigDef.Importance.HIGH;
import static org.apache.kafka.common.config.ConfigDef.Importance.LOW;
import static org.apache.kafka.common.config.ConfigDef.Importance.MEDIUM;
import static org.apache.kafka.common.config.ConfigDef.NO_DEFAULT_VALUE;

/**
 * Configures the connection between Kafka and MongoDB.
 */
public class MongoDbSinkConnector extends SinkConnector {
    private static final Logger log = LoggerFactory.getLogger(MongoDbSinkConnector.class);

    public static final String MONGO_HOST = "mongo.host";
    public static final String MONGO_PORT = "mongo.port";
    public static final String MONGO_USERNAME = "mongo.username";
    public static final String MONGO_PASSWORD = "mongo.password";
    public static final String MONGO_DATABASE = "mongo.database";
    public static final String BUFFER_CAPACITY = "buffer.capacity";
    public static final String COLLECTION_FORMAT = "mongo.collection.format";
    public static final String RECORD_CONVERTER = "record.converter.class";

    private Map<String, String> connectorConfig;

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        List<String> errorMessages = new ArrayList<>();
        for (ConfigValue v : config().validate(props)) {
            if (!v.errorMessages().isEmpty()) {
                errorMessages.add("Property " + v.name() + " with value " + v.value()
                        + " does not validate: " + String.join("; ", v.errorMessages()));
            }
        }
        if (!errorMessages.isEmpty()) {
            throw new ConnectException("Configuration does not validate: \n\t"
                    + String.join("\n\t", errorMessages));
        }

        connectorConfig = new HashMap<>(props);
        log.info(Utility.convertConfigToString(connectorConfig));
    }

    @Override
    public Class<? extends Task> taskClass() {
        return MongoDbSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        log.info("At most {} will be started", maxTasks);

        return Collections.nCopies(maxTasks, connectorConfig);
    }

    @Override
    public void stop() {
        log.debug("Stop");
        // Nothing to do since it has no background monitoring.
    }

    @Override
    public ConfigDef config() {
        ConfigDef conf = new ConfigDef();
        conf.define(MONGO_HOST, ConfigDef.Type.STRING, NO_DEFAULT_VALUE, HIGH,
                "MongoDB host name to write data to", "MongoDB", 0, ConfigDef.Width.MEDIUM,
                "MongoDB hostname");
        conf.define(MONGO_PORT, ConfigDef.Type.INT, 27017, ConfigDef.Range.atLeast(1), LOW,
                "MongoDB port", "MongoDB", 1, ConfigDef.Width.SHORT, "MongoDB port");
        conf.define(MONGO_DATABASE, ConfigDef.Type.STRING, NO_DEFAULT_VALUE, HIGH,
                "MongoDB database name", "MongoDB", 2, ConfigDef.Width.SHORT, "MongoDB database");
        conf.define(MONGO_USERNAME, ConfigDef.Type.STRING, null, MEDIUM,
                "Username to connect to MongoDB database. If not set, no credentials are used.",
                "MongoDB", 3, ConfigDef.Width.SHORT, "MongoDB username",
                Collections.singletonList(MONGO_PASSWORD));
        conf.define(MONGO_PASSWORD, ConfigDef.Type.STRING, null, MEDIUM,
                "Password to connect to MongoDB database. If not set, no credentials are used.",
                "MongoDB", 4, ConfigDef.Width.SHORT, "MongoDB password",
                Collections.singletonList(MONGO_USERNAME));
        conf.define(COLLECTION_FORMAT, ConfigDef.Type.STRING, "{$topic}", MEDIUM,
                "A format string for the destination collection name, which may contain `${topic}`"
                + "as a placeholder for the originating topic name.\n"
                + "For example, `kafka_${topic}` for the topic `orders` will map to the "
                + "collection name `kafka_orders`.", "MongoDB", 5, ConfigDef.Width.LONG,
                "MongoDB collection name format");
        conf.define(TOPICS_CONFIG, ConfigDef.Type.LIST, NO_DEFAULT_VALUE, HIGH,
                "List of topics to be streamed.");
        conf.define(BUFFER_CAPACITY, ConfigDef.Type.INT, 20_000, ConfigDef.Range.atLeast(1), LOW,
                "Maximum number of items in a MongoDB writer buffer. Once the buffer becomes full,"
                + "the task fails.");
        conf.define(RECORD_CONVERTER, ConfigDef.Type.CLASS, RecordConverterFactory.class,
                ValidClass.isSubclassOf(RecordConverterFactory.class), MEDIUM,
                "RecordConverterFactory that returns classes to convert Kafka SinkRecords to BSON "
                + "documents.");
        return conf;
    }

    public static void main(String... args) {
        System.out.println(new MongoDbSinkConnector().config().toHtmlTable());
    }
}
