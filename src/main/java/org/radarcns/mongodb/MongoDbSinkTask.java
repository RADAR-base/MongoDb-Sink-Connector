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

package org.radarcns.mongodb;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.radarcns.serialization.RecordConverter;
import org.radarcns.util.Monitor;
import org.radarcns.util.Utility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static org.radarcns.mongodb.MongoDbSinkConnector.BUFFER_CAPACITY;
import static org.radarcns.mongodb.MongoDbSinkConnector.RECORD_CONVERTERS;

/**
 * Task to handle data coming from Kafka and send it to MongoDB.
 *
 * It uses a buffer and a separate MongoDbWriter thread to achieve asynchronous writes. The buffer
 * is of fixed size (defined by {@link MongoDbSinkConnector#BUFFER_CAPACITY}) so if the MongoDB
 * connection is slower than data is being put from Kafka, the buffer will fill up. The put
 * operation will then at some point timeout.
 */
public class MongoDbSinkTask extends SinkTask {
    private static final Logger log = LoggerFactory.getLogger(MongoDbSinkTask.class);

    private final AtomicInteger count;

    private BlockingQueue<SinkRecord> buffer;
    private MongoDbWriter writer;
    private Timer timer;

    public MongoDbSinkTask() {
        count = new AtomicInteger(0);
    }

    @Override
    public String version() {
        return new MongoDbSinkConnector().version();
    }

    @Override
    public void start(Map<String, String> props) {
        timer = new Timer();
        timer.schedule(new Monitor(log, count, "have been processed"), 0, 30_000);

        AbstractConfig config = new AbstractConfig(new MongoDbSinkConnector().config().parse(props));

        buffer = new ArrayBlockingQueue<>(config.getInt(BUFFER_CAPACITY));

        List<RecordConverter> mongoConverters = Utility.loadRecordConverters(
                getClass().getClassLoader(), config.getList(RECORD_CONVERTERS));
        MongoWrapper mongoHelper = new MongoWrapper(config);

        writer = new MongoDbWriter(mongoHelper, buffer, mongoConverters, timer);
        writer.start();
    }

    @Override
    public void put(Collection<SinkRecord> sinkRecords) {
        for (SinkRecord record : sinkRecords) {
            buffer.add(record);
            count.incrementAndGet();
        }
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
        writer.flush(offsets);
    }

    @Override
    public void stop() {
        writer.close();
        timer.purge();
    }
}