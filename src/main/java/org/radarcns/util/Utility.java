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

import org.apache.kafka.connect.data.Struct;
import org.bson.BsonDouble;
import org.bson.Document;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class Utility {
    public static String convertConfigToString(Map<String, String> map){
        String ret = "User configuration are: ";
        for (Map.Entry<String, String> entry : map.entrySet()) {
            ret += "\n\t" + entry.getKey() + ": " + entry.getValue();
        }
        return ret;
    }

    public static List<Document> extractQuartile(List<Double> component){
        return Arrays.asList(
                new Document("25", new BsonDouble(component.get(0))),
                new Document("50", new BsonDouble(component.get(1))),
                new Document("75", new BsonDouble(component.get(2))));
    }

    public static String intervalKeyToMongoKey(Struct key) {
        return key.get("userID")
                + "-" + key.get("sourceID")
                + "-" + key.get("start")
                + "-" + key.get("end");
    }
}
