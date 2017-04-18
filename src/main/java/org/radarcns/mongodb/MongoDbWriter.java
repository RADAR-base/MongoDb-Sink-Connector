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

import com.mongodb.MongoException;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoIterable;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.bson.Document;
import org.radarcns.serialization.RecordConverter;
import org.radarcns.serialization.RecordConverterFactory;
import org.radarcns.util.DurationTimer;
import org.radarcns.util.Monitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A thread that reads Kafka SinkRecords from a buffer and writes them to a MongoDB database.
 *
 * <p>It keeps track of the latest offsets of records that have been written, so that a flush
 * operation can be done against specific Kafka offsets.
 */
public class MongoDbWriter implements Closeable, Runnable {
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

        retrieveOffsets();
    }

    @Override
    public void run() {
        log.info("Started MongoDbWriter");

        while (!stopping.get()) {
            SinkRecord record;
            try {
                record = buffer.take();
            } catch (InterruptedException ex) {
                log.debug("Interrupted while polling buffer", ex);
                continue;
            }
            TopicPartition partition = new TopicPartition(record.topic(), record.kafkaPartition());
            // Do not write same record twice
            Long offsetWritten = latestOffsets.get(partition);
            if (offsetWritten != null && offsetWritten >= record.kafkaOffset()) {
                continue;
            }

            store(record, 0);
            processedRecord(record, partition);
        }

        mongoHelper.close();

        log.info("Stopped MongoDbWriter");
    }

    private void store(SinkRecord record, int tries) {
        try {
            Document doc = getDoc(record);
            mongoHelper.store(record.topic(), doc);
            monitor.increment();
        } catch (DataException ex) {
            log.error("Unsupported MongoDB data type in data from Kafka. Skipping record {}",
                    record, ex);
            setException(ex);
        } catch (Exception ex) {
            if (tries + 1 < NUM_RETRIES) {
                log.error("Exception while trying to add record {}, retrying", record, ex);
                store(record, tries + 1);
            } else {
                setException(ex);
                log.error("Exception while trying to add record {}, skipping", record, ex);
            }
        }
    }

    private synchronized void processedRecord(SinkRecord record, TopicPartition topicPartition) {
        latestOffsets.put(topicPartition,record.kafkaOffset());
        notifyAll();
    }

    private synchronized void setException(Throwable ex) {
        this.exception = ex;
    }

    private Document getDoc(SinkRecord record) throws DataException {
        RecordConverter converter = converterFactory.getRecordConverter(record);

        try {
            return converter.convert(record);
        } catch (Exception ex) {
            log.error("Error while converting {}.", record, ex);
            throw new DataException("Record cannot be converted to a Document", ex);
        }
    }

    /**
     * Flushes the buffer.
     * @param offsets offsets up to which to flush.
     * @throws ConnectException if the writer is interrupted.
     */
    public synchronized void flush(Map<TopicPartition, Long> offsets)
            throws ConnectException {

        log.debug("Init flush-writer");
        if (exception != null) {
            log.error("MongoDB writer is in an illegal state");
            throw new ConnectException("MongoDB writer is in an illegal state", exception);
        }

        DurationTimer timer = new DurationTimer();
        logOffsets(offsets);

        List<Map.Entry<TopicPartition, Long>> waiting = new ArrayList<>(offsets.entrySet());

        while (true) {
            Iterator<Map.Entry<TopicPartition, Long>> waitingIterator = waiting.iterator();

            while (waitingIterator.hasNext()) {
                Map.Entry<TopicPartition, Long> partitionOffset = waitingIterator.next();
                Long offsetWritten = latestOffsets.get(partitionOffset.getKey());

                if (offsetWritten != null && offsetWritten >= partitionOffset.getValue()) {
                    waitingIterator.remove();
                }
            }

            if (waiting.isEmpty()) {
                log.info("[FLUSH-WRITER] Time-elapsed: {} s", timer.duration());
                log.debug("End flush-writer");

                break;
            }

            try {
                // wait for additional messages to be processed
                wait();
            } catch (InterruptedException ex) {
                throw new ConnectException("MongoDB writer was interrupted", ex);
            }
        }
        storeOffsets();
    }

    private void logOffsets(Map<TopicPartition, Long> offsets) {
        if (log.isDebugEnabled()) {
            log.debug("Kafka Offsets: {}", offsets.size());
            for (TopicPartition partition : offsets.keySet()) {
                log.debug("{} - {}", partition, offsets.get(partition));
            }
            log.debug("LatestOffset: {}", latestOffsets.size());
            for (TopicPartition partition : latestOffsets.keySet()) {
                log.debug("{} - {}", partition, latestOffsets.get(partition));
            }
        }
    }

    private void storeOffsets() {
        try {
            for (Map.Entry<TopicPartition, Long> offset : latestOffsets.entrySet()) {
                Document id = new Document();
                id.put("topic", offset.getKey().topic());
                id.put("partition", offset.getKey().partition());
                Document doc = new Document();
                doc.put("_id", id);
                doc.put("offset", offset.getValue());
                mongoHelper.store("OFFSETS", doc);
            }
        } catch (MongoException ex) {
            log.warn("Failed to store offsets to MongoDB", ex);
        }
    }

    private void retrieveOffsets() {
        MongoIterable<Document> documentIterable = mongoHelper.getDocuments("OFFSETS");
        try (MongoCursor<Document> documents = documentIterable.iterator()) {
            while (documents.hasNext()) {
                Document doc = documents.next();
                Document id = (Document) doc.get("_id");
                String topic = id.getString("topic");
                int partition = id.getInteger("partition");
                long offset = doc.getLong("offset");

                latestOffsets.put(new TopicPartition(topic, partition), offset);
            }
        }
    }

    /**
     * Closes the writer.
     *
     * <p>This will eventually close the thread but it will not wait for it. It will also not flush
     * the buffer.
     */
    @Override
    public void close() {
        log.debug("Closing MongoDB writer");
        stopping.set(true);
    }
}
