package org.radarcns.connect.util;

import com.mongodb.MongoClientException;
import com.mongodb.MongoClientURI;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

public class ValidMongoUri implements ConfigDef.Validator {
    @Override
    public void ensureValid(String name, Object value) {
        if (value == null || value.toString().trim().isEmpty()) {
            throw new ConfigException("URL is empty");
        }
        String uri = value.toString().trim();
        MongoClientURI mongoUri;
        try {
            mongoUri = new MongoClientURI(value.toString());
        } catch (IllegalArgumentException | MongoClientException ex) {
            throw new ConfigException("Cannot parse URI " + uri, ex);
        }

        if (mongoUri.getHosts().isEmpty() || mongoUri.getHosts().get(0).isEmpty()) {
            throw new ConfigException("MongoDB URI does not specify host");
        }

        if (mongoUri.getDatabase() == null) {
            throw new ConfigException("MongoDB URI does not specify database");
        }
    }

    @Override
    public String toString() {
        return "(mongodb|mongodb+srv)://[<user>:<password>@]<host>[:<port>]/<database>";
    }
}
