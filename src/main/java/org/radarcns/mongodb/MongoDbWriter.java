package org.radarcns.mongodb;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.bson.Document;
import org.radarcns.serialization.RecordConverter;
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
import java.util.concurrent.atomic.AtomicInteger;

import javax.activation.UnsupportedDataTypeException;

/**
 * A thread that reads Kafka SinkRecords from a buffer and writes them to a MongoDB database.
 *
 * It keeps track of the latest offsets of records that have been written, so that a flush operation
 * can be done against specific Kafka offsets.
 */
public class MongoDbWriter extends Thread implements Closeable {
    private static final Logger log = LoggerFactory.getLogger(MongoDbWriter.class);
    private static final int NUM_RETRIES = 3;

    private final AtomicInteger count;
    private final MongoWrapper mongoHelper;
    private final Map<String, RecordConverter<Document>> converterMapping;
    private final BlockingQueue<SinkRecord> buffer;

    private final AtomicBoolean stopping;
    private final Map<TopicPartition, Long> latestOffsets;
    private Throwable exception;

    /**
     * Creates a writer with a MongoDB client.
     *
     * @param props sink properties
     * @param buffer buffer
     * @param converters converters from records to a MongoDB document
     * @param timer timer to run a monitoring task on
     * @throws ConnectException if cannot connect to the MongoDB database.
     */
    public MongoDbWriter(Map<String, String> props, BlockingQueue<SinkRecord> buffer,
                         List<RecordConverter<Document>> converters, Timer timer)
            throws ConnectException {
        this.buffer = buffer;
        count = new AtomicInteger(0);

        timer.schedule(new Monitor(log, count, "have been written in MongoDB", this.buffer), 0, 30000);

        latestOffsets = new HashMap<>();
        stopping = new AtomicBoolean(false);

        mongoHelper = new MongoWrapper(props);

        if (!mongoHelper.checkConnection()) {
            mongoHelper.close();
            throw new ConnectException("Cannot connect to MongoDB database");
        }

        converterMapping = new HashMap<>();
        for (RecordConverter<Document> converter : converters) {
            for (String supportedSchema : converter.supportedSchemaNames()) {
                converterMapping.put(supportedSchema, converter);
            }
        }

        exception = null;
    }

    @Override
    public void run() {
        while (!stopping.get()) {
            SinkRecord record;
            try {
                record = buffer.take();
            } catch (InterruptedException e) {
                log.warn("Interrupted while polling buffer", e);
                continue;
            }
            store(record, 0);
            processedRecord(record);
        }

        if (mongoHelper != null) {
            mongoHelper.close();
        }

        log.info("Writer DONE!");
    }

    private void store(SinkRecord record, int tries) {
        try {
            Document doc = getDoc(record);
            mongoHelper.store(record.topic(), doc);
            count.incrementAndGet();
        } catch (UnsupportedDataTypeException e) {
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

    private Document getDoc(SinkRecord record) throws UnsupportedDataTypeException {
        RecordConverter<Document> converter = converterMapping.get(record.valueSchema().name());
        if (converter == null) {
            throw new UnsupportedDataTypeException(record.valueSchema() + " is not supported yet.");
        }

        try {
            return converter.convert(record);
        } catch (Exception e) {
            log.error("Error while converting {}.", record, e);
            throw new UnsupportedDataTypeException("Record cannot be converted to a Document");
        }
    }

    /**
     * Flushes the buffer.
     * @param offsets offsets up to which to flush.
     * @throws ConnectException if the writer is interrupted.
     */
    public synchronized void flush(Map<TopicPartition, OffsetAndMetadata> offsets)
            throws ConnectException {
        if (exception != null) {
            log.error("MongoDB writer is on illegal state");
            throw new ConnectException("MongoDB writer is on illegal state", exception);
        }

        try {
            List<TopicPartition> waiting = new ArrayList<>(offsets.keySet());
            while (true) {
                Iterator<TopicPartition> waitingIterator = waiting.iterator();
                while (waitingIterator.hasNext()) {
                    TopicPartition topicPartition = waitingIterator.next();
                    Long offset = latestOffsets.get(topicPartition);
                    if (offset != null && offset >= offsets.get(topicPartition).offset()) {
                        waitingIterator.remove();
                    }
                }
                if (waiting.isEmpty()) {
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
        log.info("Writer is shutting down");
        stopping.set(true);
        interrupt();
    }
}
