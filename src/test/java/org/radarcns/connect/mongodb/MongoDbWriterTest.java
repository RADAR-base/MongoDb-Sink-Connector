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

package org.radarcns.connect.mongodb;

import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoIterable;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.bson.Document;
import org.junit.Test;
import org.radarcns.connect.mongodb.serialization.RecordConverter;
import org.radarcns.connect.mongodb.serialization.RecordConverterFactory;
import org.radarcns.connect.util.Monitor;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Timer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Stream;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class MongoDbWriterTest {

    @Test
    public void run() throws Exception {
        MongoWrapper wrapper = mock(MongoWrapper.class);
        when(wrapper.checkConnection()).thenReturn(true);
        @SuppressWarnings("unchecked")
        MongoIterable<Document> iterable = mock(MongoIterable.class);
        @SuppressWarnings("unchecked")
        MongoCursor<Document> iterator = mock(MongoCursor.class);
        when(iterable.iterator()).thenReturn(iterator);
        when(iterator.hasNext()).thenReturn(true, false);
        Document id = new Document();
        id.put("topic", "mytopic");
        id.put("partition", 5);
        Document partDoc = new Document();
        partDoc.put("_id", id);
        partDoc.put("offset", 999L);

        when(iterator.next()).thenReturn(partDoc);
        when(wrapper.getDocuments("OFFSETS", false)).thenReturn(iterable);

        BlockingQueue<SinkRecord> buffer = new LinkedBlockingQueue<>();

        RecordConverterFactory factory = new RecordConverterFactory() {
            @Override
            protected List<RecordConverter> genericConverters() {
                return Collections.singletonList(new RecordConverter() {
                    @Override
                    public Collection<String> supportedSchemaNames() {
                        return Arrays.asList("string", "int32-int");
                    }

                    @Override
                    public Document convert(SinkRecord record) throws DataException {
                        return new Document("mykey", record.value().toString());
                    }
                });
            }
        };

        Timer timer = mock(Timer.class);

        MongoDbWriter writer = new MongoDbWriter(wrapper, buffer,
                "OFFSETS",1000, 1, factory, timer);
        verify(timer).schedule(any(Monitor.class), eq(0L), eq(30_000L));

        Thread writerThread = new Thread(writer, "MongoDB-writer");
        writerThread.start();

        buffer.add(new SinkRecord("mytopic", 5,
                null, null,
                SchemaBuilder.string().build(), "ignored", 999));
        buffer.add(new SinkRecord("mytopic", 5,
                SchemaBuilder.int32().build(), 1,
                SchemaBuilder.int32().name("int").build(), 2, 1000));
        buffer.add(new SinkRecord("mytopic", 5,
                null, null,
                SchemaBuilder.string().build(), "hi", 1001));

        writer.flush(Collections.singletonMap(
                new TopicPartition("mytopic", 5), 1001L));

        verify(wrapper, atMost(3)).store(any(), any(Stream.class));
        partDoc.put("offset", 1001L);
        verify(wrapper).store("OFFSETS", partDoc);

        writer.close();
        writerThread.interrupt();
        writerThread.join();

        verify(wrapper).close();
    }
}