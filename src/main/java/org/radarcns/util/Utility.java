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

package org.radarcns.util;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.data.Struct;
import org.bson.BsonDouble;
import org.bson.Document;
import org.radarcns.serialization.RecordConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Utility {
    private static final Logger log = LoggerFactory.getLogger(Utility.class);

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

    public static List<Document> extractQuartile(List<Double> component){
        return Arrays.asList(
                new Document("25", new BsonDouble(component.get(0))),
                new Document("50", new BsonDouble(component.get(1))),
                new Document("75", new BsonDouble(component.get(2))));
    }

    public static String intervalKeyToMongoKey(Struct key) {
        return key.get("userID") + "-" + key.get("sourceID") + "-"
                + key.get("start") + "-"+ key.get("end");
    }

    public static Map<String, String> parseArrayConfig(AbstractConfig config, String key) {
        List<String> value = config.getList(key);
        Set<String> originalKeys = config.values().keySet();
        Map<String, String> properties = new HashMap<>();
        for (String returnKey : value) {
            String returnValue = null;
            if (originalKeys.contains(returnKey)) {
                String tmpValue = config.getString(returnKey);
                if (!tmpValue.isEmpty()) {
                    returnValue = tmpValue;
                }
            }
            properties.put(returnKey, returnValue);
        }
        return properties;
    }

    public static List<RecordConverter> loadRecordConverters(ClassLoader classLoader,
                                                             List<String> converterClasses) {
        List<RecordConverter> converters = new ArrayList<>(converterClasses.size());
        for (String converterClass : converterClasses) {
            try {
                Class<?> cls = classLoader.loadClass(converterClass);
                converters.add((RecordConverter)cls.newInstance());
            } catch (ClassNotFoundException | InstantiationException | IllegalAccessException
                    | ClassCastException e) {
                log.error("Cannot instantiate RecordConverter class '{}'", converterClass, e);
            }
        }
        return converters;
    }
}
