package org.radarcns.util;

import org.apache.kafka.connect.data.Struct;
import org.bson.BsonDouble;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

/**
 * Created by Francesco Nobilia on 28/11/2016.
 */
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

    public static Set<String> stringToSet(@Nonnull String value){
        if (value.isEmpty()) {
            return Collections.emptySet();
        }
        return new HashSet<>(Arrays.asList(value.split(",")));
    }

    public static LinkedList<Document> extractQuartile(@Nonnull List<Double> component){
        LinkedList<Document> quartile = new LinkedList<>();

        quartile.addLast(new Document("25", new BsonDouble(component.get(0))));
        quartile.addLast(new Document("50", new BsonDouble(component.get(1))));
        quartile.addLast(new Document("75", new BsonDouble(component.get(2))));

        return quartile;
    }

    public static int getInt(@Nonnull Map<String, String> props, @Nonnull String key,
                             int defaultValue) {
        String valueString = props.get(key);
        if (valueString != null) {
            try {
                return Integer.parseInt(valueString);
            } catch (NumberFormatException ex) {
                log.warn("Property {} = {} cannot be parsed as an integer. " +
                        "Using default value {}", key, valueString, defaultValue, ex);
            }
        }
        return defaultValue;
    }

    public static String intervalKeyToMongoKey(@Nonnull Struct key) {
        return key.get("userID") + "-" + key.get("sourceID") + "-"
                + key.get("start") + "-"+ key.get("end");
    }
}
