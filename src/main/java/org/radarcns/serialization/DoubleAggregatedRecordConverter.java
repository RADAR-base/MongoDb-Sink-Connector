package org.radarcns.serialization;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.bson.BsonDateTime;
import org.bson.BsonDouble;
import org.bson.Document;
import org.radarcns.util.Utility;

import java.util.Collection;
import java.util.Collections;

import javax.annotation.Nonnull;

public class DoubleAggregatedRecordConverter implements RecordConverter {
    @Override
    public Collection<String> supportedSchemaNames() {
        return Collections.singleton(org.radarcns.aggregator.DoubleAggegator.class.getName());
    }

    @Override
    public Document convert(@Nonnull SinkRecord record) {
        Struct key = (Struct) record.key();
        Struct value = (Struct) record.value();

        return new Document("_id", Utility.intervalKeyToMongoKey(key))
                .append("user", key.getString("userID"))
                .append("source", key.getString("sourceID"))
                .append("min", new BsonDouble(value.getFloat64("min")))
                .append("max", new BsonDouble(value.getFloat64("max")))
                .append("sum", new BsonDouble(value.getFloat64("sum")))
                .append("count", new BsonDouble(value.getFloat64("count")))
                .append("avg", new BsonDouble(value.getFloat64("avg")))
                .append("quartile", Utility.extractQuartile(value.getArray("quartile")))
                .append("iqr", new BsonDouble(value.getFloat64("iqr")))
                .append("start", new BsonDateTime(key.getInt64("start")))
                .append("end", new BsonDateTime(key.getInt64("end")));
    }
}
