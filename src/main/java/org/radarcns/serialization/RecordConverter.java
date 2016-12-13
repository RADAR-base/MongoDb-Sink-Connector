package org.radarcns.serialization;

import org.apache.kafka.connect.sink.SinkRecord;
import org.bson.Document;

import java.util.Collection;

import javax.annotation.Nonnull;

/**
 * Converts Kafka records to MongoDB documents
 */
public interface RecordConverter {
    Collection<String> supportedSchemaNames();
    Document convert(@Nonnull SinkRecord record);
}
