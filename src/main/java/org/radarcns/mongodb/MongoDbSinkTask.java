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

package org.radarcns.mongodb;

import static org.radarcns.mongodb.MongoDbSinkConnector.BUFFER_CAPACITY;
import static org.radarcns.mongodb.MongoDbSinkConnector.RECORD_CONVERTER;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.IllegalWorkerStateException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.radarcns.serialization.RecordConverterFactory;
import org.radarcns.util.DurationTimer;
import org.radarcns.util.Monitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Task to handle data coming from Kafka and send it to MongoDB.
 *
 * <p>It uses a buffer and a separate MongoDbWriter thread to achieve asynchronous writes. The
 * buffer is of fixed size (defined by {@link MongoDbSinkConnector#BUFFER_CAPACITY}) so if the
 * MongoDB connection is slower than data is being put from Kafka, the buffer will fill up. The put
 * operation will then at some point timeout.
 */
public class MongoDbSinkTask extends SinkTask {
    private static final Logger log = LoggerFactory.getLogger(MongoDbSinkTask.class);
    private Monitor monitor;
    private DurationTimer actionTimer;
    private final Map<TopicPartition, Long> latestOffsetPut;

    private BlockingQueue<SinkRecord> buffer;
    private MongoDbWriter writer;
    private Thread writerThread;
    private Timer timerThread;

    public MongoDbSinkTask() {
        monitor = new Monitor(log, "have been processed");
        actionTimer = new DurationTimer();
        latestOffsetPut = new HashMap<>();
    }

    @Override
    public String version() {
        return new MongoDbSinkConnector().version();
    }

    @Override
    public void start(Map<String, String> props) {
        timerThread = new Timer();
        timerThread.schedule(monitor, 0, 30_000);

        AbstractConfig config = new AbstractConfig(
                new MongoDbSinkConnector().config().parse(props));

        buffer = new ArrayBlockingQueue<>(config.getInt(BUFFER_CAPACITY));

        RecordConverterFactory converterFactory;
        try {
            converterFactory = (RecordConverterFactory)config.getClass(RECORD_CONVERTER)
                    .newInstance();
        } catch (InstantiationException | IllegalAccessException | ClassCastException ex) {
            throw new IllegalWorkerStateException("Got illegal RecordConverterClass", ex);
        }
        writer = createMongoDbWriter(config, buffer, converterFactory, timerThread);
        writerThread = new Thread(writer, "MongDB-writer");
        writerThread.start();
    }

    /**
     * Helper function to create a {@link MongoDbWriter} instance
     * @param config object
     * @param buffer buffer of the records
     * @param converterFactory of available converters
     * @param timer for writer
     * @return a {@link MongoDbWriter} object
     * @throws ConnectException
     */
    public MongoDbWriter createMongoDbWriter(AbstractConfig config,
                                      BlockingQueue<SinkRecord> buffer,
                                      RecordConverterFactory converterFactory, Timer timer)
            throws ConnectException {
        MongoWrapper mongoHelper = new MongoWrapper(config, null);

        return new MongoDbWriter(mongoHelper, buffer, converterFactory, timer);
    }

    @Override
    public void put(Collection<SinkRecord> sinkRecords) {
        if(writer !=null) {
            if (log.isDebugEnabled()) {
                log.debug("Init put");
                actionTimer.reset();
            }

            for (SinkRecord record : sinkRecords) {
                TopicPartition partition = new TopicPartition(record.topic(),
                        record.kafkaPartition());
                latestOffsetPut.put(partition, record.kafkaOffset());
                buffer.add(record);
                monitor.increment();

                if (log.isDebugEnabled()) {
                    log.debug("{} --> {}", partition, record.kafkaOffset());
                }
            }

            if (log.isDebugEnabled()) {
                log.debug("[PUT] Time elapsed: {} s", actionTimer.duration());
                log.debug("End put");
            }
        }
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
        if(writer !=null) {
            log.debug("Init flush");
            actionTimer.reset();

            Map<TopicPartition, Long> offsetsPut = new HashMap<>();
            for (TopicPartition partition : offsets.keySet()) {
                Long offset = latestOffsetPut.get(partition);
                if (offset != null) {
                    offsetsPut.put(partition, offset);
                }
            }
            writer.flush(offsetsPut);

            log.info("[FLUSH] Time elapsed: {} s", actionTimer.duration());
            log.debug("End flush");
        }
    }

    @Override
    public void stop() {
        log.info("Stopping MongoDBSinkTask");
        if (writer != null) {
            writer.close();
        }
        if (writerThread != null) {
            writerThread.interrupt();
            try {
                writerThread.join(30_000L);
            } catch (InterruptedException ex) {
                log.info("Failed to wait for writer thread to finish.", ex);
            }
        }
        timerThread.cancel();

        //clean initialized resources
        monitor.cancel();
        monitor = null;

        actionTimer = null;
        log.info("Stoped MongoDBSinkTask");
    }
}