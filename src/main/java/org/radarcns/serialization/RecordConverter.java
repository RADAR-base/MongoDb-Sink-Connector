package org.radarcns.serialization;

import org.apache.kafka.connect.sink.SinkRecord;
import org.bson.Document;

import java.util.Collection;

import javax.annotation.Nonnull;

/**
 * Converts Kafka records to MongoDB documents
 */
public interface RecordConverter {
    /**
     * Collection of Avro schema names supported by this RecordConverter.
     *
     * The schema names used are the fully qualified (including namespace) and case-sensitive names.
     * If the converter requires records with both a key and a value schema, the returned format is
     * "KeySchemaName-ValueSchemaName". If the key is not required, only "ValueSchemaName" may be
     * returned.
     */
    Collection<String> supportedSchemaNames();

    /**
     * Convert a Kafka record to a BSON document.
     * @param record record to convert
     * @return BSON document
     */
    Document convert(@Nonnull SinkRecord record);
}
