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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RecordConverterFactory {
    private final Map<String, RecordConverter> genericConverterMap;

    public RecordConverterFactory() {
        this.genericConverterMap = new HashMap<>();
        for (RecordConverter converter : genericConverters()) {
            for (String supportedSchema : converter.supportedSchemaNames()) {
                genericConverterMap.put(supportedSchema, converter);
            }
        }
    }

    protected List<RecordConverter> genericConverters() {
        return Arrays.asList(
                new AggregatedAccelerationRecordConverter(),
                new DoubleAggregatedRecordConverter());
    }

    public RecordConverter getRecordConverter(SinkRecord record)
            throws DataException {
        RecordConverter converter = null;

        if (record.valueSchema() == null) {
            throw new DataException("Cannot process data from topic "
                    + record.topic() + " without a schema");
        }
        if (record.keySchema() != null) {
            converter = genericConverterMap.get(record.keySchema().name() + "-"
                    + record.valueSchema().name());
        }
        if (converter == null) {
            converter = genericConverterMap.get(record.valueSchema().name());
        }
        if (converter == null) {
            throw new DataException("Cannot find a suitable RecordConverter class "
                    + "for record with schema " + record.valueSchema()
                    + " in topic " + record.topic());
        }
        return converter;
    }
}
