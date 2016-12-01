package org.radarcns.mongodb;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.bson.Document;
import org.radarcns.util.MongoHelper;
import org.radarcns.util.Monitor;
import org.radarcns.util.Utility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import javax.activation.UnsupportedDataTypeException;

/**
 * Created by Francesco Nobilia on 30/11/2016.
 */
public class MongoDbWriter implements Runnable{

    private static final Logger log = LoggerFactory.getLogger(MongoDbWriter.class);

    private final AtomicInteger count;

    private final MongoHelper mongoHelper;

    private final Map<String,String> collectorMapping;

    private final LinkedBlockingDeque<SinkRecord> buffer;

    private final long timeoutBuffer = 30000;
    private final TimeUnit unit = TimeUnit.MILLISECONDS;

    private final Timer timer;

    private final AtomicBoolean stopper;

    private final AtomicBoolean empty;

    private final AtomicBoolean exception;

    public MongoDbWriter(Map<String, String> props, LinkedBlockingDeque<SinkRecord> buffer){
        this.buffer = buffer;
        count = new AtomicInteger(0);

        mongoHelper = new MongoHelper(props);

        collectorMapping = new HashMap<>();
        collectorMapping.put(props.get(MongoDbSinkConnector.COLL_DOUBLE_ARRAY),MongoDbSinkConnector.COLL_DOUBLE_ARRAY);
        collectorMapping.put(props.get(MongoDbSinkConnector.COLL_DOUBLE_SINGLETON),MongoDbSinkConnector.COLL_DOUBLE_SINGLETON);

        timer = new Timer();
        timer.schedule(new Monitor(count,"have been written in MongoDB",log), 0,30000);

        stopper = new AtomicBoolean(false);

        empty = new AtomicBoolean(true);

        exception = new AtomicBoolean(false);
    }

    @Override
    public void run() {
        while (true) {
            SinkRecord record = null;
            try {
                record = buffer.pollLast(timeoutBuffer, unit);

                if (record == null) {
                    log.info("Nothing to flush");

                    empty.set(true);

                    if(stopper.get()){
                        break;
                    }

                } else {
                    empty.set(false);
                    count.incrementAndGet();

                    Document doc = getDoc(record);

                    mongoHelper.store(record.topic(), doc);
                }
            }
            catch (InterruptedException e) {
                log.warn(e.getMessage());
            } catch (UnsupportedDataTypeException e) {
                log.error(e.getMessage());
                exception.set(true);
            } catch (Exception e){
                log.error(e.getMessage());
                if(record != null){
                    buffer.addLast(record);
                }
                exception.set(true);
            }
        }

        if(mongoHelper != null) {
            mongoHelper.close();
        }

        timer.purge();
        log.info("Writer DONE!");
    }

    private Document getDoc(SinkRecord record) throws UnsupportedDataTypeException {
        String aggregator = collectorMapping.get(record.valueSchema().name());
        if(aggregator == null){
            throw new UnsupportedDataTypeException(record.valueSchema()+" is not supported yet.");
        }

        switch (aggregator){
            case MongoDbSinkConnector.COLL_DOUBLE_SINGLETON:
                try {
                    return Utility.doubleAggToDoc(record);
                }
                catch (Exception e){
                    log.error("Error while converting {}.",record.toString(),e);
                    throw new UnsupportedDataTypeException("Record cannot be converted in Document");
                }
            case MongoDbSinkConnector.COLL_DOUBLE_ARRAY:
                try {
                    return Utility.accelerometerToDoc(record);
                }
                catch (Exception e){
                    log.error(e.getMessage());
                    log.error("Error while converting {}.",record.toString(),e);
                    throw new UnsupportedDataTypeException("Record cannot be converted in Document");
                }
            default:
                throw new UnsupportedDataTypeException("Record cannot be converted in Document. Missing mapping. "+record.toString());
        }
    }

    public void flushing(){
        if(exception.get()){
            log.error("Writer is on illegal state");
            throw new ConnectException("Writer is on illegal state");
        }

        while(!empty.get());

        return;
    }

    public void shutdown(){
        log.info("Writer is shutting down");
        stopper.set(true);
    }

    public static MongoDbWriter start(Map<String, String> props, LinkedBlockingDeque<SinkRecord> buffer){
        MongoDbWriter instance = new MongoDbWriter(props,buffer);

        new Thread(instance).start();

        return instance;
    }

}
