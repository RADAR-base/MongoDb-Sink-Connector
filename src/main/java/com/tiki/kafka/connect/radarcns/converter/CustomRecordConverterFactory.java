package com.tiki.kafka.connect.radarcns.converter;

import java.util.ArrayList;
import java.util.List;
import org.radarcns.connect.mongodb.serialization.RecordConverter;
import org.radarcns.connect.mongodb.serialization.RecordConverterFactory;

/**
 *
 * @author phu.nguyen3
 */
/**
 * Extended RecordConverterFactory to allow customized RecordConverter class
 * that are needed
 */
public class CustomRecordConverterFactory extends RecordConverterFactory {

    /**
     * Overrides genericConverter to append custom RecordConverter class to
     * RecordConverterFactory
     *
     * @return list of RecordConverters available
     */
    @Override
    protected List<RecordConverter> genericConverters() {
        List<RecordConverter> recordConverters = new ArrayList<>();
        recordConverters.addAll(super.genericConverters());
        recordConverters.add(new Map2BsonConverter());
        return recordConverters;
    }

}
