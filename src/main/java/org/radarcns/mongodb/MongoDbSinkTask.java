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
import org.bson.Document;
import org.radarcns.aggregator.DoubleAggegator;
import org.radarcns.aggregator.DoubleArrayAggegator;
import org.radarcns.key.WindowedKey;
import org.radarcns.util.MongoHelper;
import org.radarcns.util.RollingTimeCount;
import org.radarcns.util.Utility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

import javax.activation.UnsupportedDataTypeException;

/**
 * FileStreamSinkTask writes records to stdout or a file.
 */
public class MongoDbSinkTask extends SinkTask {

    private static final Logger log = LoggerFactory.getLogger(MongoDbSinkTask.class);

    private MongoHelper mongoHelper;

    private LinkedList<SinkRecord> buffer;
    private int bufferSize;

    private Map<String,String> collectorMapping;

    private RollingTimeCount timeAverage;

    public MongoDbSinkTask() {}

    @Override
    public String version() {
        return new MongoDbSinkConnector().version();
    }

    @Override
    public void start(Map<String, String> props) {
        if(validateConfig(props)){
            mongoHelper = new MongoHelper(props);

            collectorMapping = new HashMap<>();
            collectorMapping.put(props.get(MongoDbSinkConnector.COLL_DOUBLE_ARRAY),MongoDbSinkConnector.COLL_DOUBLE_ARRAY);
            collectorMapping.put(props.get(MongoDbSinkConnector.COLL_DOUBLE_SINGLETON),MongoDbSinkConnector.COLL_DOUBLE_SINGLETON);

            buffer = new LinkedList<>();
            bufferSize = Integer.parseInt(props.get(MongoDbSinkConnector.BATCH_SIZE));

            timeAverage = new RollingTimeCount(30000);
        }
    }

    @Override
    public void put(Collection<SinkRecord> sinkRecords) {
        for (SinkRecord record : sinkRecords) {
            buffer.addLast(record);
            timeAverage.increment();

            if(buffer.size() == bufferSize){
                flushBuffer();
            }
        }

        if(!buffer.isEmpty()){
            flushBuffer();
        }

        if(timeAverage.hasCount()) {
            log.info("{} analysed records", timeAverage.getCount());
        }
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
        flushBuffer();
    }

    @Override
    public void stop() {
        flushBuffer();
        if(mongoHelper != null) {
            mongoHelper.close();
        }
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

    private void flushBuffer(){
        try {
            if (buffer != null) {
                if (buffer.isEmpty()) {
                    log.info("Nothing to flush");
                } else {
                    log.info("Flushing buffer");
                    while (!buffer.isEmpty()) {
                        SinkRecord record = buffer.removeFirst();

                        log.info(record.valueSchema().name());

                        Document doc = getDoc(record);

                        log.info(doc.toString());

//                        mongoHelper.store(record.topic(), doc);
                    }
                }
            } else {
                log.info("Buffer is null, a null buffer cannot be flushed");
            }
        }
        catch (UnsupportedDataTypeException e){
            log.error(e.getMessage());
        }
        catch (Exception e){
            log.error(e.getMessage());
            mongoHelper.close();

            throw e;
        }
    }

    private Document getDoc(SinkRecord record) throws UnsupportedDataTypeException {
        String aggregator = collectorMapping.get(record.valueSchema().name());
        if(aggregator == null){
            throw new UnsupportedDataTypeException(record.valueSchema()+" is not supported yet.");
        }

        switch (aggregator){
            case MongoDbSinkConnector.COLL_DOUBLE_SINGLETON:
                try {
                    WindowedKey key = (WindowedKey)record.key();
                    DoubleAggegator value = (DoubleAggegator)record.value();

                    return Utility.doubleAggToDoc(key,value);
                }
                catch (Exception e){
                    log.error("Error while converting {}.",record.toString(),e);
                    throw new UnsupportedDataTypeException("Record cannot be converted in Document");
                }
            case MongoDbSinkConnector.COLL_DOUBLE_ARRAY:
                try {
                    WindowedKey key = (WindowedKey)record.key();
                    DoubleArrayAggegator value = (DoubleArrayAggegator)record.value();

                    return Utility.accelerometerToDoc(key,value);
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

}