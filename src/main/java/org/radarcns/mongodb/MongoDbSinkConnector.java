package org.radarcns.mongodb;

import com.google.common.base.Strings;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkConnector;
import org.radarcns.util.Utility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
    public static final String RECORD_CONVERTERS = "record.converter.classes";

    public static final String[] REQUIRED_PROPERTIES = {
        MONGO_HOST, MONGO_PORT, MONGO_USERNAME, MONGO_PASSWORD, MONGO_DATABASE, TOPICS_CONFIG,
        RECORD_CONVERTERS,
    };

    private Map<String, String> connectorConfig;

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        connectorConfig = new HashMap<>(props);

        for (String req : REQUIRED_PROPERTIES) {
            if (Strings.isNullOrEmpty(connectorConfig.get(req))) {
                throw new ConnectException("Required connector property '" + req + "' is not set");
            }
        }
        if (Utility.parseArrayConfig(connectorConfig, TOPICS_CONFIG) == null) {
            throw new ConnectException("Not all topics in the '" + TOPICS_CONFIG
                    + "' property have a topic to database name mapping");
        }

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
        return null;
    }
}
