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
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.bson.Document;
import org.radarcns.serialization.RecordConverter;
import org.radarcns.serialization.RecordConverterFactory;
import org.radarcns.util.Monitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A thread that reads Kafka SinkRecords from a buffer and writes them to a MongoDB database.
 *
 * It keeps track of the latest offsets of records that have been written, so that a flush operation
 * can be done against specific Kafka offsets.
 */
public class MongoDbWriter extends Thread implements Closeable {
    private static final Logger log = LoggerFactory.getLogger(MongoDbWriter.class);
    private static final int NUM_RETRIES = 3;

    private final MongoWrapper mongoHelper;
    private final BlockingQueue<SinkRecord> buffer;

    private final AtomicBoolean stopping;
    private final Map<TopicPartition, Long> latestOffsets;
    private final RecordConverterFactory converterFactory;
    private final Monitor monitor;
    private Throwable exception;

    /**
     * Creates a writer with a MongoDB client.
     *
     * @param mongoHelper MongoDB connection
     * @param buffer buffer
     * @param converterFactory converters factory for converters from records to MongoDB documents
     * @param timer timer to run a monitoring task on
     * @throws ConnectException if cannot connect to the MongoDB database.
     */
    public MongoDbWriter(MongoWrapper mongoHelper, BlockingQueue<SinkRecord> buffer,
                         RecordConverterFactory converterFactory, Timer timer)
            throws ConnectException {
        super("MongoDB-writer");
        this.buffer = buffer;
        this.monitor = new Monitor(log, "have been written in MongoDB", this.buffer);
        timer.schedule(monitor, 0, 30_000);

        latestOffsets = new HashMap<>();
        stopping = new AtomicBoolean(false);

        this.mongoHelper = mongoHelper;

        if (!mongoHelper.checkConnection()) {
            mongoHelper.close();
            throw new ConnectException("Cannot connect to MongoDB database");
        }

        this.converterFactory = converterFactory;

        exception = null;
    }

    @Override
    public void run() {
        log.info("Started MongoDbWriter");

        while (!stopping.get()) {
            SinkRecord record;
            try {
                record = buffer.take();
            } catch (InterruptedException e) {
                log.debug("Interrupted while polling buffer", e);
                continue;
            }
            store(record, 0);
            processedRecord(record);
        }

        mongoHelper.close();

        log.info("Stopped MongoDbWriter");
    }

    private void store(SinkRecord record, int tries) {
        try {
            Document doc = getDoc(record);
            mongoHelper.store(record.topic(), doc);
            monitor.increment();
        } catch (DataException e) {
            log.error("Unsupported MongoDB data type in data from Kafka. Skipping record {}",
                    record, e);
            setException(e);
        } catch (Exception e){
            tries++;
            if (tries < NUM_RETRIES) {
                log.error("Exception while trying to add record {}, retrying", record, e);
                store(record, tries);
            } else {
                setException(e);
                log.error("Exception while trying to add record {}, skipping", record, e);
            }
        }
    }

    private synchronized void processedRecord(SinkRecord record) {
        TopicPartition topicPartition = new TopicPartition(record.topic(), record.kafkaPartition());
        latestOffsets.put(topicPartition,record.kafkaOffset());
        notify();
    }

    private synchronized void setException(Throwable ex) {
        this.exception = ex;
    }

    private Document getDoc(SinkRecord record) throws DataException {
        RecordConverter converter = converterFactory.getRecordConverter(record);

        try {
            return converter.convert(record);
        } catch (Exception e) {
            log.error("Error while converting {}.", record, e);
            throw new DataException("Record cannot be converted to a Document", e);
        }
    }

    /**
     * Flushes the buffer.
     * @param offsets offsets up to which to flush.
     * @throws ConnectException if the writer is interrupted.
     */
    public synchronized void flush(Map<TopicPartition, OffsetAndMetadata> offsets)
            throws ConnectException {

        log.debug("Init flush-writer");
        long startTime = System.nanoTime();

        if (exception != null) {
            log.error("MongoDB writer is on illegal state");
            throw new ConnectException("MongoDB writer is on illegal state", exception);
        }

        log.debug("Kafka Offsets: {}", offsets.size());
        for (TopicPartition tPart : offsets.keySet()) {
            log.debug("{} - {}", tPart.toString(), offsets.get(tPart).toString());
        }
        log.debug("LatestOffset: {}", latestOffsets.size());
        for (TopicPartition tPart : latestOffsets.keySet()) {
            log.debug("{} - {}", tPart.toString(), latestOffsets.get(tPart).toString());
        }

        try {
            List<TopicPartition> waiting = new ArrayList<>(offsets.keySet());
            while (true) {
                Iterator<TopicPartition> waitingIterator = waiting.iterator();

                while (waitingIterator.hasNext()) {
                    TopicPartition topicPartition = waitingIterator.next();
                    Long offset = latestOffsets.get(topicPartition);

                    if (offset != null && (offset + 1) >= offsets.get(topicPartition).offset()) {
                        waitingIterator.remove();
                    }
                    else if(offset == null && offsets.get(topicPartition).offset() == 0){
                        waitingIterator.remove();
                    }
                }

                if (waiting.isEmpty()) {
                    long endTime = System.nanoTime();
                    log.info("[FLUSH-WRITER] Time-laps: {}nsec", endTime - startTime);
                    log.debug("End flush-writer");

                    return;
                }

                // wait for additional messages to be processed
                wait();
            }
        } catch (InterruptedException ex) {
            throw new ConnectException("MongoDB writer was interrupted", ex);
        }
    }

    /**
     * Closes the writer.
     *
     * This will eventually close the thread but it will not wait for it. It will also not flush
     * the buffer.
     */
    @Override
    public void close() {
        log.debug("Closing MongoDB writer");
        stopping.set(true);
        interrupt();
    }
}
