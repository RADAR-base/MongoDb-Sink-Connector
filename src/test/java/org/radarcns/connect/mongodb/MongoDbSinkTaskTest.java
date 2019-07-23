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

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.radarcns.connect.mongodb.serialization.RecordConverterFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.radarcns.connect.mongodb.MongoDbSinkConnector.BUFFER_CAPACITY;
import static org.radarcns.connect.mongodb.MongoDbSinkConnector.MONGO_URI;
import static org.radarcns.connect.mongodb.MongoDbSinkConnector.RECORD_CONVERTER;
import static org.radarcns.connect.mongodb.MongoDbSinkConnector.TOPICS_CONFIG;

public class MongoDbSinkTaskTest {

    @Test
    public void start() {
        MongoDbSinkTask sinkTask = spy(MongoDbSinkTask.class);
        MongoDbWriter writer = mock(MongoDbWriter.class);
        CreateMongoDbAnswer answer = new CreateMongoDbAnswer(writer);

        //noinspection unchecked
        doAnswer(answer).when(sinkTask)
                .createMongoDbWriter(any(), any(), any(), anyInt(), anyLong(), any(), any());

        Map<String, String> config = new HashMap<>();
        config.put(MONGO_URI, "mongodb://localhost/db");
        config.put(TOPICS_CONFIG, "t");
        config.put(BUFFER_CAPACITY, String.valueOf(10));
        config.put(RECORD_CONVERTER, RecordConverterFactory.class.getName());
        sinkTask.start(config);

        assertEquals(1, answer.timesCalled);
        assertEquals(10, answer.foundBuffer.remainingCapacity());
        assertEquals(RecordConverterFactory.class, answer.foundFactory.getClass());

        SinkRecord record = new SinkRecord("t", 1, Schema.STRING_SCHEMA, "k",
                Schema.STRING_SCHEMA, "v", 1000L);
        sinkTask.put(Collections.singleton(record));
        assertEquals(9, answer.foundBuffer.remainingCapacity());

        SinkRecord foundRecord = answer.foundBuffer.poll();
        assertEquals(record, foundRecord);

        assertEquals(10, answer.foundBuffer.remainingCapacity());

        sinkTask.stop();
    }

    private static class CreateMongoDbAnswer implements Answer<MongoDbWriter> {
        private BlockingQueue<SinkRecord> foundBuffer;
        private RecordConverterFactory foundFactory;
        private int timesCalled;
        private MongoDbWriter writer;

        private CreateMongoDbAnswer(MongoDbWriter writer) {
            this.writer = writer;
        }

        @Override
        public MongoDbWriter answer(InvocationOnMock invocation) {
            foundBuffer = invocation.getArgument(1);
            foundFactory = invocation.getArgument(4);
            timesCalled++;
            return writer;
        }
    }
}