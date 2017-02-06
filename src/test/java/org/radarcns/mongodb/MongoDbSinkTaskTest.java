package org.radarcns.mongodb;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.radarcns.serialization.RecordConverterFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;
import static org.radarcns.mongodb.MongoDbSinkConnector.*;

public class MongoDbSinkTaskTest {

    @Test
    public void start() throws Exception {
        MongoDbSinkTask sinkTask = spy(MongoDbSinkTask.class);
        MongoDbWriter writer = mock(MongoDbWriter.class);
        CreateMongoDbAnswer answer = new CreateMongoDbAnswer(writer);

        //noinspection unchecked
        doAnswer(answer).when(sinkTask)
                .createMongoDbWriter(any(), any(), any(), any());

        Map<String, String> config = new HashMap<>();
        config.put(MONGO_DATABASE, "db");
        config.put(MONGO_HOST, "localhost");
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
        public MongoDbWriter answer(InvocationOnMock invocation) throws Throwable {
            foundBuffer = invocation.getArgument(1);
            foundFactory = invocation.getArgument(2);
            timesCalled++;
            return writer;
        }
    }
}