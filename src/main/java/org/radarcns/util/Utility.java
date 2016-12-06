package org.radarcns.util;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.bson.BsonDateTime;
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
import java.util.stream.Collectors;

/**
 * Created by Francesco Nobilia on 28/11/2016.
 */
public class Utility {

    private static final Logger log = LoggerFactory.getLogger(Utility.class);

    public static String convertConfigToString(Map<String,String> map){
        return "User configuration are: \n\t"+map.keySet().stream().map(key -> key+": "+map.get(key)).collect(Collectors.joining("\n\t"));
    }

    public static Set<String> getTopicSet(String value){
        return new HashSet<>(Arrays.asList(value.split(",")));
    }

    public static String keyListToString(Map<String,String> map){
        return map.keySet().stream().map(Object::toString).collect(Collectors.joining(","));
    }

    public static Set<String> getMustHaveSet(String value){
        return new HashSet<>(Arrays.asList(value.split(",")));
    }

    public static Set<String> stringToSet(String value){
        return new HashSet<>(Arrays.asList(value.split(",")));
    }

    public static Document doubleAggToDoc(SinkRecord record){

        Struct key = (Struct) record.key();
        Struct value = (Struct) record.value();

        String mongoId = key.getString("userID")+"-"+
                key.getString("sourceID")+"-"+
                key.getInt64("start")+"-"+
                key.getInt64("end");

        Document doc = new Document("_id", mongoId)
                .append("user", key.getString("userID"))
                .append("source", key.getString("sourceID"))
                .append("min", new BsonDouble(value.getFloat64("min")))
                .append("max", new BsonDouble(value.getFloat64("max")))
                .append("sum", new BsonDouble(value.getFloat64("sum")))
                .append("count", new BsonDouble(value.getFloat64("count")))
                .append("avg", new BsonDouble(value.getFloat64("avg")))
                .append("quartile", extractQuartile(value.getArray("quartile")))
                .append("iqr", new BsonDouble(value.getFloat64("iqr")))
                .append("start", new BsonDateTime(key.getInt64("start")))
                .append("end", new BsonDateTime(key.getInt64("end")));

        return doc;
    }

    public static Document accelerometerToDoc(SinkRecord record){
        Struct key = (Struct) record.key();
        Struct value = (Struct) record.value();

        String mongoId = key.getString("userID")+"-"+
                key.getString("sourceID")+"-"+
                key.getInt64("start")+"-"+
                key.getInt64("end");

        Document doc = new Document("_id", mongoId)
                .append("user", key.getString("userID"))
                .append("source", key.getString("sourceID"))
                .append("min", accCompToDoc(value.getArray("min")))
                .append("max", accCompToDoc(value.getArray("max")))
                .append("sum", accCompToDoc(value.getArray("sum")))
                .append("count", accCompToDoc(value.getArray("count")))
                .append("avg", accCompToDoc(value.getArray("avg")))
                .append("quartile", accQuartileToDoc(value.getArray("quartile")))
                .append("iqr", accCompToDoc(value.getArray("iqr")))
                .append("start", new BsonDateTime(key.getInt64("start")))
                .append("end", new BsonDateTime(key.getInt64("end")));

        return doc;
    }

    private static LinkedList<Document> extractQuartile(List<Double> component){
        LinkedList<Document> quartile = new LinkedList<>();

        quartile.addLast(new Document("25", new BsonDouble(component.get(0))));
        quartile.addLast(new Document("50", new BsonDouble(component.get(1))));
        quartile.addLast(new Document("75", new BsonDouble(component.get(2))));

        return quartile;
    }

    private static Document accCompToDoc(List<Double> component){
        return new Document("x", component.get(0)).append("y", component.get(1)).append("z", component.get(2));
    }

    private static Document accQuartileToDoc(List<List<Double>> list){
        return new Document("x", extractQuartile(list.get(0)))
                        .append("y", extractQuartile(list.get(1)))
                        .append("z", extractQuartile(list.get(2)));
    }
}
