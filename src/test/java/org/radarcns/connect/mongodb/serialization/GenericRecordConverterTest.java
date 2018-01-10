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

package org.radarcns.connect.mongodb.serialization;

import static org.junit.Assert.assertEquals;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.Document;
import org.junit.Before;
import org.junit.Test;

public class GenericRecordConverterTest {

    private GenericRecordConverter converter;
    private Document expected;

    @Before
    public void setUp() {
        converter = new GenericRecordConverter();
        expected = new Document();
    }

    @Test
    public void convertString() throws Exception {
        expected.put("value", new BsonString("mystring"));

        SinkRecord record = new SinkRecord(null, 0, null, null,
                SchemaBuilder.string().build(), "mystring", 0L);
        assertEquals(expected, converter.convert(record));
    }

    @Test
    public void convertIntString() {
        expected.put("_id", "10");
        expected.put("key", new BsonInt32(10));
        expected.put("value", new BsonString("mystring"));

        SinkRecord record = new SinkRecord(null, 0, SchemaBuilder.int32().build(), 10,
                SchemaBuilder.string().build(), "mystring", 0L);
        assertEquals(expected, converter.convert(record));
    }

    @Test
    public void convertIntBoolStringInt() {
        Schema keySchema = SchemaBuilder.struct()
                .field("a", SchemaBuilder.int32().build())
                .field("b", SchemaBuilder.bool().build())
                .build();
        Struct key = new Struct(keySchema);
        key.put("a", 10);
        key.put("b", true);

        Schema valueSchema = SchemaBuilder.struct()
                .field("c", SchemaBuilder.string().build())
                .field("d", SchemaBuilder.int32().build())
                .build();
        Struct value = new Struct(valueSchema);
        value.put("c", "mystring");
        value.put("d", 30);

        expected.put("_id", "{10-true}");
        expected.put("key", new BsonDocument()
                .append("a", new BsonInt32(10))
                .append("b", new BsonBoolean(true)));
        expected.put("value", new BsonDocument()
                .append("c", new BsonString("mystring"))
                .append("d", new BsonInt32(30)));

        SinkRecord record = new SinkRecord(null, 0, keySchema, key, valueSchema, value, 0L);
        assertEquals(expected, converter.convert(record));
    }
}