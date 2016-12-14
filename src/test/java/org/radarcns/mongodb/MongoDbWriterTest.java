package org.radarcns.mongodb;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.bson.BsonString;
import org.bson.Document;
import org.junit.Test;
import org.radarcns.serialization.RecordConverter;
import org.radarcns.serialization.RecordConverterFactory;
import org.radarcns.util.Monitor;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Timer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class MongoDbWriterTest {
    @Test
    public void run() throws Exception {
        BlockingQueue<SinkRecord> buffer = new LinkedBlockingQueue<>();
        Timer timer = mock(Timer.class);
        RecordConverter converter = new RecordConverter() {
            @Override
            public Collection<String> supportedSchemaNames() {
                return Arrays.asList("string", "int32-int");
            }
            @Override
            public Document convert(SinkRecord record) throws DataException {

                return new Document("mykey", new BsonString(record.value().toString()));
            }
        };
        RecordConverterFactory factory = new RecordConverterFactory() {
            @Override
            protected List<RecordConverter> genericConverters() {
                return Collections.singletonList(converter);
            }
        };
        MongoWrapper wrapper = mock(MongoWrapper.class);
        when(wrapper.checkConnection()).thenReturn(true);

        MongoDbWriter writer = new MongoDbWriter(wrapper, buffer, factory, timer);
        verify(timer).schedule(any(Monitor.class), eq(0L), eq(30_000L));

        buffer.add(new SinkRecord("mytopic", 5,
                SchemaBuilder.int32().build(), 1,
                SchemaBuilder.int32().name("int").build(), 2, 1000));
        buffer.add(new SinkRecord("mytopic", 5,
                null, null,
                SchemaBuilder.string().build(), "hi", 1001));

        writer.start();
        writer.flush(Collections.singletonMap(
                new TopicPartition("mytopic", 5), new OffsetAndMetadata(1000)));

        verify(wrapper).store("mytopic", new Document("mykey", new BsonString("2")));
        verify(wrapper).store("mytopic", new Document("mykey", new BsonString("hi")));

        writer.close();
        writer.join();

        verify(wrapper).close();
    }
}