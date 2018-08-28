package com.tiki.kafka.connect.radarcns.converter;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.radarcns.connect.mongodb.serialization.RecordConverter;


public class Map2BsonConverter implements RecordConverter {

    /**
     * Returns the list of supported schemas, which behaves as the id to select
     * suitable RecordConverter for a SinkRecord.
     *
     * @return a list of supported Schemas
     */
    @Override
    public Collection<String> supportedSchemaNames() {
        return Collections.singleton(null);
    }

    /**
     * Converts a ServerStatus SinkRecord into a MongoDB Document.
     *
     * @param sinkRecord record to be converted
     * @return converted MongoDB Document to write
     */
    @Override
    public Document convert(SinkRecord sinkRecord) throws DataException {

        Map value = (Map) sinkRecord.value();
        String key = (String) sinkRecord.key();
        MessageDigest digest;
        try {
            digest = java.security.MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException ex) {
            throw new DataException(ex);
        }
        digest.update(key.getBytes());
        return new Document(value)
                .append("_id", new ObjectId(digest.digest()));
    }

}
