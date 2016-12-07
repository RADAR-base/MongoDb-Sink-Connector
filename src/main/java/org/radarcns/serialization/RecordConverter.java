package org.radarcns.serialization;

import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Collection;

/**
 * Converts Kafka records to MongoDB documents
 */
public interface RecordConverter<T> {
    Collection<String> supportedSchemaNames();
    T convert(SinkRecord record);
}
