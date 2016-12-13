package org.radarcns.mongodb;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.bson.Document;
import org.radarcns.serialization.RecordConverter;
import org.radarcns.util.Monitor;
import org.radarcns.util.Utility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
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
    // Assuming record sizes of 1 kB, we default to a 20 MB buffer
    private static final int DEFAULT_BUFFER_CAPACITY = 20_000;

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
        int bufferCapacity = Utility.getInt(props, BUFFER_CAPACITY, DEFAULT_BUFFER_CAPACITY);
        buffer = new ArrayBlockingQueue<>(bufferCapacity);

        List<RecordConverter> mongoConverters = loadRecordConverters(
                props.get(RECORD_CONVERTERS));

        timer = new Timer();
        timer.schedule(new Monitor(log, count, "have been processed"), 0, 30000);

        writer = new MongoDbWriter(props, buffer, mongoConverters, timer);
        writer.start();
    }

    private List<RecordConverter> loadRecordConverters(String property) {
        ClassLoader classLoader = getClass().getClassLoader();
        String[] converterClasses = Utility.splitByComma(property);

        List<RecordConverter> converters = new ArrayList<>(converterClasses.length);
        for (String converterClass : converterClasses) {
            try {
                RecordConverter converter = (RecordConverter)classLoader.loadClass(converterClass).newInstance();
                converters.add(converter);
            } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | ClassCastException e) {
                e.printStackTrace();
            }
        }
        return converters;
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