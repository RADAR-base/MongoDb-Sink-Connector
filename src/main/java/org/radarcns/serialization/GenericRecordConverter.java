/*
 * Copyright 2017 The Hyve and King's College London
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

import java.util.Collection;
import java.util.Collections;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.Document;

/**
 * Generically convert SinkRecords to BSON Documents.
 *
 * <p>The key of a {@link SinkRecord} is converted to String and becomes the Document _id. The value
 * is converted to BsonValue. If it is a primitive value, it is assigned to the "value" property.
 * If it is a Map or Struct, the values are directly entered in the document with the same
 * key-values as the originating Struct or Map.
 */
public class GenericRecordConverter implements RecordConverter {
    @Override
    public Collection<String> supportedSchemaNames() {
        return Collections.singleton(null);
    }

    @Override
    public Document convert(SinkRecord record) throws DataException {
        // determine ID from key, if any
        Document document;
        Object key = record.key();
        if (key != null) {
            document = new Document("_id", JavaBsonType.objectToString(key));
        } else {
            document = new Document();
        }

        Object value = record.value();
        if (value != null) {
            BsonValue bson = JavaBsonType.objectToBson(value);
            if (bson instanceof BsonDocument) {
                document.putAll((BsonDocument) bson);
            } else {
                document.put("value", bson);
            }
        }

        return document;
    }
}
