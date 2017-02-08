package org.radarcns.serialization;

import static org.junit.Assert.*;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.sink.SinkRecord;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.Document;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by joris on 08/02/2017.
 */
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
        SinkRecord record = new SinkRecord(null, 0, null, null,
                SchemaBuilder.string().build(), "mystring", 0L);
        Document result = converter.convert(record);

        expected.put("value", new BsonString("mystring"));
        assertEquals(expected, result);
    }

    @Test
    public void convertIntString() {
        SinkRecord record = new SinkRecord(null, 0, SchemaBuilder.int32().build(), 10,
                SchemaBuilder.string().build(), "mystring", 0L);
        Document result = converter.convert(record);

        expected.put("_id", "10");
        expected.put("value", new BsonString("mystring"));
        assertEquals(expected, result);
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

        SinkRecord record = new SinkRecord(null, 0, keySchema, key, valueSchema, value, 0L);
        Document result = converter.convert(record);

        expected.put("_id", "{10-true}");
        expected.put("c", new BsonString("mystring"));
        expected.put("d", new BsonInt32(30));
        assertEquals(expected, result);
    }
}