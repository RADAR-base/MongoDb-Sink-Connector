package org.radarcns.serialization;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.bson.BsonDateTime;
import org.bson.Document;
import org.radarcns.util.Utility;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import javax.annotation.Nonnull;

import static org.radarcns.mongodb.MongoDbSinkConnector.COLL_DOUBLE_ARRAY;

public class AggregatedAccelerationRecordConverter implements RecordConverter<Document> {
    @Override
    public Collection<String> supportedSchemaNames() {
        return Collections.singleton(COLL_DOUBLE_ARRAY);
    }

    @Override
    public Document convert(@Nonnull SinkRecord record) {
        Struct key = (Struct) record.key();
        Struct value = (Struct) record.value();

        return new Document("_id", Utility.intervalKeyToMongoKey(key))
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
    }

    private static Document accCompToDoc(List<Double> component){
        return new Document("x", component.get(0))
                .append("y", component.get(1))
                .append("z", component.get(2));
    }

    private static Document accQuartileToDoc(List<List<Double>> list){
        return new Document("x", Utility.extractQuartile(list.get(0)))
                .append("y", Utility.extractQuartile(list.get(1)))
                .append("z", Utility.extractQuartile(list.get(2)));
    }
}
