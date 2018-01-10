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

package org.radarcns.mongodb;

import static org.apache.kafka.common.config.ConfigDef.Importance.HIGH;
import static org.apache.kafka.common.config.ConfigDef.Importance.LOW;
import static org.apache.kafka.common.config.ConfigDef.Importance.MEDIUM;
import static org.apache.kafka.common.config.ConfigDef.NO_DEFAULT_VALUE;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.radarcns.serialization.RecordConverterFactory;
import org.radarcns.util.NotEmptyString;
import org.radarcns.util.Utility;
import org.radarcns.util.ValidClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Configures the connection between Kafka and MongoDB.
 */
public class MongoDbSinkConnector extends SinkConnector {
    private static final Logger logger = LoggerFactory.getLogger(MongoDbSinkConnector.class);

    public static final String MONGO_GROUP = "MongoDB";
    public static final String MONGO_HOST = "mongo.host";
    public static final String MONGO_PORT = "mongo.port";
    public static final int MONGO_PORT_DEFAULT = 27017;
    public static final String MONGO_USERNAME = "mongo.username";
    public static final String MONGO_PASSWORD = "mongo.password";
    public static final String MONGO_DATABASE = "mongo.database";
    public static final String BUFFER_CAPACITY = "buffer.capacity";
    public static final int BUFFER_CAPACITY_DEFAULT = 20_000;
    public static final String COLLECTION_FORMAT = "mongo.collection.format";
    public static final String RECORD_CONVERTER = "record.converter.class";

    static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(MONGO_HOST, Type.STRING, NO_DEFAULT_VALUE, new NotEmptyString(), HIGH,
                "MongoDB host name to write data to", MONGO_GROUP, 0, ConfigDef.Width.MEDIUM,
                "MongoDB hostname")
            .define(MONGO_PORT, Type.INT, MONGO_PORT_DEFAULT, ConfigDef.Range.atLeast(1),
                LOW, "MongoDB port", MONGO_GROUP, 1, ConfigDef.Width.SHORT, "MongoDB port")
            .define(MONGO_DATABASE, Type.STRING, NO_DEFAULT_VALUE, new NotEmptyString(),
                HIGH, "MongoDB database name", MONGO_GROUP, 2, ConfigDef.Width.SHORT,
                "MongoDB database")
            .define(MONGO_USERNAME, Type.STRING, null, MEDIUM,
                "Username to connect to MongoDB database. If not set, no credentials are used.",
                    MONGO_GROUP, 3, ConfigDef.Width.SHORT, "MongoDB username",
                Collections.singletonList(MONGO_PASSWORD))
            .define(MONGO_PASSWORD, Type.STRING, null, MEDIUM,
                "Password to connect to MongoDB database. If not set, no credentials are used.",
                    MONGO_GROUP, 4, ConfigDef.Width.SHORT, "MongoDB password",
                Collections.singletonList(MONGO_USERNAME))
            .define(COLLECTION_FORMAT, Type.STRING, "{$topic}", new NotEmptyString(), MEDIUM,
                "A format string for the destination collection name, which may contain "
                + "`${topic}` as a placeholder for the originating topic name.\n"
                + "For example, `kafka_${topic}` for the topic `orders` will map to the "
                + "collection name `kafka_orders`.", MONGO_GROUP, 5, ConfigDef.Width.LONG,
                "MongoDB collection name format")
            .define(TOPICS_CONFIG, Type.LIST, NO_DEFAULT_VALUE, HIGH,
                "List of topics to be streamed.")
            .define(BUFFER_CAPACITY, Type.INT, BUFFER_CAPACITY_DEFAULT,
                ConfigDef.Range.atLeast(1), LOW,
                "Maximum number of items in a MongoDB writer buffer. Once the buffer becomes "
                + "full, the task fails.")
            .define(RECORD_CONVERTER, Type.CLASS, RecordConverterFactory.class,
                ValidClass.isSubclassOf(RecordConverterFactory.class), MEDIUM,
                "RecordConverterFactory that returns classes to convert Kafka SinkRecords to "
                + "BSON documents.");
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
            throw new ConfigException("Configuration does not validate: \n\t"
                    + String.join("\n\t", errorMessages));
        }

        connectorConfig = new HashMap<>(props);
        logger.info(Utility.convertConfigToString(connectorConfig));
    }

    @Override
    public Class<? extends Task> taskClass() {
        return MongoDbSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        logger.info("At most {} will be started", maxTasks);

        return Collections.nCopies(maxTasks, connectorConfig);
    }

    @Override
    public void stop() {
        logger.debug("Stop");
        // Nothing to do since it has no background monitoring.
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    public static void main(String... args) {
        logger.info("Configuration table: \n{}", new MongoDbSinkConnector().config().toHtmlTable());
    }
}
