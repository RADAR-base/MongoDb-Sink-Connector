package org.radarcns.util;

import org.bson.BsonDateTime;
import org.bson.BsonDouble;
import org.bson.Document;
import org.radarcns.aggregator.DoubleAggegator;
import org.radarcns.aggregator.DoubleArrayAggegator;
import org.radarcns.key.WindowedKey;

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

    public static Document doubleAggToDoc(WindowedKey key, DoubleAggegator value){
        String mongoId = key.getUserID()+"-"+key.getSourceID()+"-"+key.getStart()+"-"+key.getEnd();

        Document doc = new Document("_id", mongoId)
                .append("user", key.getUserID())
                .append("source", key.getSourceID())
                .append("min", new BsonDouble(value.getMin()))
                .append("max", new BsonDouble(value.getMax()))
                .append("sum", new BsonDouble(value.getSum()))
                .append("count", new BsonDouble(value.getCount()))
                .append("avg", new BsonDouble(value.getAvg()))
                .append("quartile", extractQuartile(value.getQuartile()))
                .append("iqr", new BsonDouble(value.getIqr()))
                .append("start", new BsonDateTime(key.getStart()))
                .append("end", new BsonDateTime(key.getEnd()));

        return doc;
    }

    public static Document accelerometerToDoc(WindowedKey key, DoubleArrayAggegator value){
        String mongoId = key.getUserID()+"-"+key.getSourceID()+"-"+key.getStart()+"-"+key.getEnd();

        Document doc = new Document("_id", mongoId)
                .append("user", key.getUserID())
                .append("source", key.getSourceID())
                .append("min", accCompToDoc(value.getMin()))
                .append("max", accCompToDoc(value.getMax()))
                .append("sum", accCompToDoc(value.getSum()))
                .append("count", accCompToDoc(value.getCount()))
                .append("avg", accCompToDoc(value.getAvg()))
                .append("quartile", accQuartileToDoc(value.getQuartile()))
                .append("iqr", accCompToDoc(value.getIqr()))
                .append("start", new BsonDateTime(key.getStart()))
                .append("end", new BsonDateTime(key.getEnd()));

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
