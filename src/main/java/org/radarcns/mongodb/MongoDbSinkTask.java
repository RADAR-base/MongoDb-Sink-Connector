package org.radarcns.mongodb;

/**
 * Created by Francesco Nobilia on 28/11/2016.
 */

import com.google.common.base.Strings;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.radarcns.util.Monitor;
import org.radarcns.util.Utility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * FileStreamSinkTask writes records to stdout or a file.
 */
public class MongoDbSinkTask extends SinkTask {

    private static final Logger log = LoggerFactory.getLogger(MongoDbSinkTask.class);

    private AtomicInteger count;

    private LinkedBlockingDeque<SinkRecord> buffer;

    private MongoDbWriter writer;

    private Timer timer;

    public MongoDbSinkTask() {}

    @Override
    public String version() {
        return new MongoDbSinkConnector().version();
    }

    @Override
    public void start(Map<String, String> props) {
        if(validateConfig(props)){
            buffer = new LinkedBlockingDeque<>();
            count = new AtomicInteger(0);

            writer = MongoDbWriter.start(props,buffer);

            timer = new Timer();
            timer.schedule(new Monitor(count,"have been processed",log), 0,30000);
        }
    }

    @Override
    public void put(Collection<SinkRecord> sinkRecords) {
        for (SinkRecord record : sinkRecords) {
            try {
                buffer.putFirst(record);
                count.incrementAndGet();
            }
            catch (InterruptedException e) {
                log.error(e.getMessage());
                throw new ConnectException("SinkRecord cannot be insert inside the queue.");
            }

        }
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
        writer.flushing();
    }

    @Override
    public void stop() {
        timer.purge();
        writer.shutdown();
    }

    private boolean validateConfig(Map<String, String> config) {
        Set<String> mustHaveList = Utility.getMustHaveSet(config.get(MongoDbSinkConnector.MUST_HAVE));

        for (String key: mustHaveList) {
            if(Strings.isNullOrEmpty(config.get(key))){
                log.error("MongoDbSinkTask cannot be created. {} is not defined",key);
                throw new ConnectException("MongoDbSinkTask cannot be created. "+key+" is not defined");
            }
        }

        return true;
    }

}