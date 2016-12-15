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

package org.radarcns.serialization;

import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.bson.Document;

import java.util.Collection;

/**
 * Converts Kafka records to MongoDB documents
 */
public interface RecordConverter {
    /**
     * Collection of {@link org.apache.kafka.connect.data.Schema} names supported by this
     * RecordConverter.
     *
     * The schema names used are the fully qualified (including namespace) and case-sensitive names.
     * If the converter requires records with both a key and a value schema, the returned format is
     * "KeySchemaName-ValueSchemaName". If the key is not required, only "ValueSchemaName" may be
     * returned. KeySchemaName and ValueSchemaName may be substituted by the Object class that it
     * supports. If the converter supports all types of data, return null.
     */
    Collection<String> supportedSchemaNames();

    /**
     * Convert a Kafka record to a BSON document.
     * @param record record to convert
     * @return BSON document
     * @throws DataException if the record cannot be converted by the current converter.
     */
    Document convert(SinkRecord record) throws DataException;
}
