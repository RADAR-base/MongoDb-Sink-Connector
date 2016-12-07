package org.radarcns.util;

import org.bson.BsonDouble;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by Francesco Nobilia on 28/11/2016.
 */
public class Utility {
    private final static Logger log = LoggerFactory.getLogger(Utility.class);

    public static String convertConfigToString(Map<String, String> map){
        String ret = "User configuration are: ";
        for (Map.Entry<String, String> entry : map.entrySet()) {
            ret += "\n\t" + entry.getKey() + ": " + entry.getValue();
        }
        return ret;
    }

    public static String keyListToString(Map<String, String> map){
        return String.join(",", map.keySet());
    }

    public static Set<String> stringToSet(String value){
        return new HashSet<>(Arrays.asList(value.split(",")));
    }

    public static LinkedList<Document> extractQuartile(List<Double> component){
        LinkedList<Document> quartile = new LinkedList<>();

        quartile.addLast(new Document("25", new BsonDouble(component.get(0))));
        quartile.addLast(new Document("50", new BsonDouble(component.get(1))));
        quartile.addLast(new Document("75", new BsonDouble(component.get(2))));

        return quartile;
    }

    public static int getInt(Map<String, String> props, String key, int defaultValue) {
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
}
