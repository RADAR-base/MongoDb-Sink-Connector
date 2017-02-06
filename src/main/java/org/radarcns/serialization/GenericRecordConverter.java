package org.radarcns.serialization;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonDateTime;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonNull;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Generically convert SinkRecords to BSON Documents.
 *
 * <p>The key of a {@link SinkRecord} is converted to String and becomes the Document _id. The value
 * is converted to BsonValue. If it is a primitive value, it is assigned to the "value" property.
 * If it is a Map or Struct, the values are directly entered in the document with the same
 * key-values as the originating Struct or Map.
 */
public class GenericRecordConverter implements RecordConverter {
    protected static final char[] hexArray = "0123456789ABCDEF".toCharArray();

    @Override
    public Collection<String> supportedSchemaNames() {
        return Collections.singleton(null);
    }

    @Override
    public Document convert(SinkRecord record) throws DataException {
        // determine ID from key, if any
        Document document;
        Object key = record.key();
        if (key != null) {
            document = new Document("_id", toString(key));
        } else {
            document = new Document();
        }

        Object value = record.value();
        if (value != null) {
            if (value instanceof Struct || value instanceof Map) {
                document.putAll((BsonDocument)toBson(value));
            } else {
                document.put("value", toBson(value));
            }
        }

        return document;
    }

    /** Convert given record value to BSON. */
    private BsonValue toBson(Object record) throws DataException {
        if (record == null) {
            return new BsonNull();
        } else if (record instanceof Float) {
            return new BsonDouble(floatToDouble((Float) record));
        } else if (record instanceof Double) {
            return new BsonDouble((Double) record);
        } else if (record instanceof Integer || record instanceof Byte || record instanceof Short) {
            return new BsonInt32(((Number) record).intValue());
        } else if (record instanceof Long) {
            return new BsonInt64((Long) record);
        } else if (record instanceof String) {
            return new BsonString((String) record);
        } else if (record instanceof byte[]) {
            return new BsonBinary((byte[]) record);
        } else if (record instanceof Struct) {
            Struct struct = (Struct) record;
            List<Field> fields = struct.schema().fields();
            BsonDocument doc = new BsonDocument();
            for (Field field : fields) {
                String key = field.name();
                Object value = struct.get(field);
                if (key.startsWith("time")) {
                    BsonDateTime dateTime = toDateTime(value);
                    if (dateTime != null) {
                        doc.put(key, dateTime);
                        continue;
                    }
                }
                doc.put(key, toBson(value));
            }
            return doc;
        } else if (record instanceof Map) {
            Map<?, ?> map = (Map<?, ?>) record;
            BsonDocument doc = new BsonDocument();
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                String key = toString(entry.getKey());
                Object value = entry.getValue();
                if (key.startsWith("time")) {
                    BsonDateTime dateTime = toDateTime(value);
                    if (dateTime != null) {
                        doc.put(key, dateTime);
                        continue;
                    }
                }
                doc.put(key, toBson(value));
            }
            return doc;
        } else if (record instanceof List) {
            List<?> list = (List<?>) record;
            BsonArray array = new BsonArray();
            for (Object o : list) {
                array.add(toBson(o));
            }
            return array;
        } else {
            throw new DataException("Data type " + record.getClass() + " not recognized");
        }
    }

    /** Stay closer to apparent value than to actual value. */
    private static double floatToDouble(float value) {
        return Double.parseDouble(Float.toString(value));
    }

    /**
     * Try to convert to BsonDateTime.
     *
     * @param obj raw value
     * @return BsonDateTime, or null if unsuccessful
     */
    private static BsonDateTime toDateTime(Object obj) {
        if (obj instanceof Long) {
            return new BsonDateTime((Long)obj);
        } else if (obj instanceof Double) {
            return new BsonDateTime((long)(1000d * (Double) obj));
        } else {
            return null;
        }
    }

    /** Convert given part of a record to String. */
    private String toString(Object record) throws DataException {
        if (record == null) {
            return "null";
        } else if (record instanceof Struct) {
            Struct struct = (Struct)record;
            List<Field> fields = struct.schema().fields();
            List<String> strings = new ArrayList<>(fields.size());
            for (Field field : fields) {
                strings.add(toString(struct.get(field)));
            }
            return "{" + String.join("-", strings) + "}";
        } else if (record instanceof Map) {
            Map<?, ?> map = (Map<?, ?>)record;
            Map<String, String> stringMap = new HashMap<>();
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                stringMap.put(toString(entry.getKey()), toString(entry.getKey()));
            }
            List<String> sortedKeys = new ArrayList<>(stringMap.keySet());
            Collections.sort(sortedKeys);
            List<String> entryStrings = new ArrayList<>(stringMap.size());
            for (String key : sortedKeys) {
                entryStrings.add(key + ":" + stringMap.get(key));
            }
            return "{" + String.join(",", entryStrings) + "}";
        } else if (record instanceof List) {
            List<?> list = (List<?>)record;
            List<String> stringList = new ArrayList<>(list.size());
            for (Object o : list) {
                stringList.add(toString(o));
            }
            return "[" + String.join(",", stringList) + "]";
        } else if (record instanceof byte[]) {
            byte[] bytes = (byte[]) record;
            char[] hexChars = new char[bytes.length * 2 + 2];
            // prepend "0x"
            hexChars[0] = '0';
            hexChars[1] = 'x';
            for (int j = 0; j < bytes.length; j++) {
                int pair = bytes[j] & 0xFF;
                hexChars[j * 2 + 2] = hexArray[pair >>> 4];
                hexChars[j * 2 + 3] = hexArray[pair & 0x0F];
            }
            return new String(hexChars);
        } else {
            return record.toString();
        }
    }
}
