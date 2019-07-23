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

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.radarcns.connect.mongodb.serialization.RecordConverterFactory;
import org.radarcns.connect.util.NotEmptyString;
import org.radarcns.connect.util.Utility;
import org.radarcns.connect.util.ValidClass;
import org.radarcns.connect.util.ValidMongoUri;
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
    private static final Logger logger = LoggerFactory.getLogger(MongoDbSinkConnector.class);

    public static final String MONGO_GROUP = "MongoDB";
    public static final String MONGO_URI = "mongo.uri";
    public static final String BUFFER_CAPACITY = "buffer.capacity";
    public static final int BUFFER_CAPACITY_DEFAULT = 20_000;
    public static final String BATCH_SIZE = "batch.size";
    public static final int BATCH_SIZE_DEFAULT = 2_500;
    public static final String BATCH_FLUSH_MS = "batch.flush.ms";
    public static final int BATCH_FLUSH_MS_DEFAULT = 15_000;
    public static final String COLLECTION_FORMAT = "mongo.collection.format";
    public static final String RECORD_CONVERTER = "record.converter.class";
    public static final String OFFSET_COLLECTION_DEFAULT = "OFFSETS";
    public static final String OFFSET_COLLECTION = "mongo.offset.collection";

    static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(MONGO_URI, Type.STRING, NO_DEFAULT_VALUE, new ValidMongoUri(), HIGH,
                "URI encoding MongoDB host, port, user (if any) and database.",
                    MONGO_GROUP, 0, ConfigDef.Width.MEDIUM, "MongoDB URI")
            .define(COLLECTION_FORMAT, Type.STRING, "{$topic}", new NotEmptyString(), MEDIUM,
                "A format string for the destination collection name, which may contain "
                + "`${topic}` as a placeholder for the originating topic name.\n"
                + "For example, `kafka_${topic}` for the topic `orders` will map to the "
                + "collection name `kafka_orders`.", MONGO_GROUP, 5, ConfigDef.Width.LONG,
                "MongoDB collection name format")
            .define(OFFSET_COLLECTION, Type.STRING, OFFSET_COLLECTION_DEFAULT,
                    MEDIUM, "The mongo collection for storage "
                            + "the latest offset processed. Default: "
                            + OFFSET_COLLECTION_DEFAULT)
            .define(TOPICS_CONFIG, Type.LIST, NO_DEFAULT_VALUE, HIGH,
                "List of topics to be streamed.")
            .define(BUFFER_CAPACITY, Type.INT, BUFFER_CAPACITY_DEFAULT,
                ConfigDef.Range.atLeast(1), LOW,
                "Maximum number of items in a MongoDB writer buffer. Once the buffer becomes "
                + "full, the task fails.")
            .define(RECORD_CONVERTER, Type.CLASS, RecordConverterFactory.class,
                ValidClass.isSubclassOf(RecordConverterFactory.class), MEDIUM,
                "RecordConverterFactory that returns classes to convert Kafka SinkRecords to "
                + "BSON documents.")
            .define(BATCH_SIZE, Type.INT, BATCH_SIZE_DEFAULT, ConfigDef.Range.atLeast(1),
                LOW, "Batch size to initiate a MongoDB write operation. If the buffer"
                            + " does not reach this capacity within batch.flush.ms, it will be"
                            + " written anyway.")
            .define(BATCH_FLUSH_MS, Type.INT, BATCH_FLUSH_MS_DEFAULT, ConfigDef.Range.atLeast(0),
                LOW, "Flush a batch after this amount of milliseconds.");
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
}
