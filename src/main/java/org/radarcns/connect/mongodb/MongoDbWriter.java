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

import com.mongodb.MongoException;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoIterable;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.bson.Document;
import org.radarcns.connect.mongodb.serialization.RecordConverter;
import org.radarcns.connect.mongodb.serialization.RecordConverterFactory;
import org.radarcns.connect.util.Monitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A thread that reads Kafka SinkRecords from a buffer and writes them to a MongoDB database.
 *
 * <p>It keeps track of the latest offsets of records that have been written, so that a flush
 * operation can be done against specific Kafka offsets.
 */
public class MongoDbWriter implements Closeable, Runnable {
    private static final Logger logger = LoggerFactory.getLogger(MongoDbWriter.class);
    private static final int NUM_RETRIES = 3;
    private final String offsetCollection;

    private final MongoWrapper mongoHelper;
    private final BlockingQueue<SinkRecord> buffer;

    private final AtomicBoolean stopping;
    private final Map<TopicPartition, Long> latestOffsets;
    private final RecordConverterFactory converterFactory;
    private final Monitor monitor;
    private final long flushMs;
    private final int maxBufferSize;
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
                         String offsetCollection, int maxBufferSize, long flushMs,
                         RecordConverterFactory converterFactory, Timer timer)
            throws ConnectException {
        this.buffer = buffer;
        this.monitor = new Monitor(logger, "have been written in MongoDB", this.buffer);
        this.offsetCollection = offsetCollection;

        timer.schedule(monitor, 0, 30_000);

        latestOffsets = new HashMap<>();
        stopping = new AtomicBoolean(false);
        this.maxBufferSize = maxBufferSize;
        this.flushMs = flushMs;

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
        logger.info("Started MongoDbWriter");

        long nextFlush = 0;
        List<SinkRecord> localBuffer = new ArrayList<>(maxBufferSize);

        while (!stopping.get()) {
            try {
                long maxPoll = nextFlush != 0 ? nextFlush - System.currentTimeMillis() : flushMs;
                SinkRecord record = buffer.poll(maxPoll, TimeUnit.MILLISECONDS);
                if (record != null) {
                    if (nextFlush == 0) {
                        nextFlush = System.currentTimeMillis() + flushMs;
                    }
                    localBuffer.add(record);
                }
            } catch (InterruptedException ex) {
                logger.debug("Interrupted while polling buffer", ex);
                continue;
            }

            if (System.currentTimeMillis() >= nextFlush || localBuffer.size() >= maxBufferSize) {
                flushLocalBuffer(localBuffer);
                localBuffer.clear();
                nextFlush = 0;
            }
        }

        mongoHelper.close();

        logger.info("Stopped MongoDbWriter");
    }

    private void flushLocalBuffer(List<SinkRecord> localBuffer) {
        localBuffer.stream()
                // if schema and/or value is null, skip the record
                .filter(e -> {
                    if (e.valueSchema() == null) {
                        logger.warn("Cannot write record with null value or schema for record {}",
                                e.toString());
                        return false;
                    } else {
                        return true;
                    }
                })
                .map(KafkaDocument::new)
                // do not write records multiple times
                .filter(e -> e.getOffset() > latestOffsets.getOrDefault(e.getPartition(), -1L))
                .collect(Collectors.groupingBy(e -> e.getPartition().topic()))
                .forEach((topic, records) -> {
                    // only write the latest value for each ID
                    Collection<KafkaDocument> docs = records.stream()
                            .collect(Collectors.toMap(
                                    KafkaDocument::getId, Function.identity(), (v1, v2) -> v2))
                            .values();

                    store(topic, docs, 0);
                    markRecordsDone(docs);
                });
    }

    private void store(String topic, Collection<KafkaDocument> records, int tries) {
        try {
            if (records.size() == 1) {
                mongoHelper.store(topic, records.iterator().next().getDocument());
                monitor.increment();
            } else {
                mongoHelper.store(topic, records.stream().map(KafkaDocument::getDocument));
                monitor.add(records.size());
            }
        } catch (DataException ex) {
            logger.error("Unsupported MongoDB data type in data from Kafka. Skipping record {}",
                    records, ex);
            setException(ex);
        } catch (Exception ex) {
            if (tries + 1 < NUM_RETRIES) {
                logger.error("Exception while trying to add record {}, retrying", records, ex);
                store(topic, records, tries + 1);
            } else {
                setException(ex);
                logger.error("Exception while trying to add record {}, skipping", records, ex);
            }
        }
    }

    private synchronized void markRecordsDone(Collection<KafkaDocument> record) {
        latestOffsets.putAll(record.stream()
                .collect(Collectors.toMap(
                        KafkaDocument::getPartition, KafkaDocument::getOffset, Math::max)));
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
            logger.error("Error while converting {}.", record, ex);
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
        if (exception != null) {
            logger.error("MongoDB writer is in an illegal state");
            throw new ConnectException("MongoDB writer is in an illegal state", exception);
        }

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
        if (logger.isDebugEnabled()) {
            logger.debug("Kafka Offsets: {}", offsets.size());
            for (TopicPartition partition : offsets.keySet()) {
                logger.debug("{} - {}", partition, offsets.get(partition));
            }
            logger.debug("LatestOffset: {}", latestOffsets.size());
            for (TopicPartition partition : latestOffsets.keySet()) {
                logger.debug("{} - {}", partition, latestOffsets.get(partition));
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
                mongoHelper.store(offsetCollection, doc);
            }
        } catch (MongoException ex) {
            logger.warn("Failed to store offsets to MongoDB", ex);
        }
    }

    private void retrieveOffsets() {
        logger.info("Retrieve offsets from collection: {}", offsetCollection);
        MongoIterable<Document> documentIterable =
                mongoHelper.getDocuments(offsetCollection, false);
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
        logger.debug("Closing MongoDB writer");
        stopping.set(true);
    }

    private class KafkaDocument {
        private final long offset;
        private final TopicPartition partition;
        private final SinkRecord record;
        private Document document;

        KafkaDocument(SinkRecord record) {
            this.record = record;
            partition = new TopicPartition(record.topic(), record.kafkaPartition());
            offset = record.kafkaOffset();
        }

        public long getOffset() {
            return offset;
        }

        public TopicPartition getPartition() {
            return partition;
        }

        public Document getDocument() {
            if (document == null) {
                document = getDoc(record);
            }
            return document;
        }

        public SinkRecord getRecord() {
            return record;
        }

        public String getId() {
            Object value = getDocument().get("_id");
            String valStr = value.toString();
            return valStr != null ? valStr  : UUID.randomUUID().toString();
        }
    }
}
