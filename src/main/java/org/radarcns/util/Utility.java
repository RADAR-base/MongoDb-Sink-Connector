package org.radarcns.util;

import com.google.common.base.Strings;

import org.apache.kafka.connect.data.Struct;
import org.bson.BsonDouble;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

public class Utility {
    private static final Logger log = LoggerFactory.getLogger(Utility.class);

    public static String convertConfigToString(@Nonnull Map<String, String> map){
        String ret = "User configuration are: ";
        for (Map.Entry<String, String> entry : map.entrySet()) {
            ret += "\n\t" + entry.getKey() + ": " + entry.getValue();
        }
        return ret;
    }

    public static String keyListToString(@Nonnull Map<String, String> map){
        return String.join(",", map.keySet());
    }

    public static String[] splitByComma(@Nonnull String value){
        if (value.isEmpty()) {
            return new String[0];
        }
        String[] result = value.split(",");
        for (int i = 0; i < result.length; i++) {
            result[i] = result[i].trim();
        }
        return result;
    }

    public static List<Document> extractQuartile(@Nonnull List<Double> component){
        return Arrays.asList(
                new Document("25", new BsonDouble(component.get(0))),
                new Document("50", new BsonDouble(component.get(1))),
                new Document("75", new BsonDouble(component.get(2))));
    }

    public static int getInt(@Nonnull Map<String, String> props, @Nonnull String key,
                             int defaultValue) {
        String valueString = props.get(key);
        if (valueString != null) {
            try {
                return Integer.parseInt(valueString);
            } catch (NumberFormatException ex) {
                log.warn("Property {} = {} cannot be parsed as an integer. "
                        + "Using default value {}", key, valueString, defaultValue, ex);
            }
        }
        return defaultValue;
    }

    public static String intervalKeyToMongoKey(@Nonnull Struct key) {
        return key.get("userID") + "-" + key.get("sourceID") + "-"
                + key.get("start") + "-"+ key.get("end");
    }

    public static Map<String, String> parseArrayConfig(Map<String, String> config, String key) {
        String value = config.get(key);
        if (value == null) {
            return null;
        }

        Map<String, String> properties = new HashMap<>();
        for (String returnKey : splitByComma(value)) {
            String returnValue = config.get(returnKey);
            if (Strings.isNullOrEmpty(returnValue)) {
                return null;
            }
            properties.put(returnKey, returnValue);
        }
        return properties;
    }
}
